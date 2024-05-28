use parking_lot::{Mutex, RwLock};
use rayexec_bullet::{batch::Batch, field::Field};
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::database::catalog::CatalogTx;
use crate::database::create::{CreateScalarFunctionInfo, CreateTableInfo, OnConflict};
use crate::database::ddl::{CreateFut, DropFut, EmptyCreateFut, SchemaModifier};
use crate::database::entry::{CatalogEntry, TableEntry};
use crate::database::schema::Schema;
use crate::{
    database::table::{DataTable, DataTableInsert, DataTableScan},
    execution::operators::{PollPull, PollPush},
};

/// Quick and dirty in-memory implementation of a schema and related data
/// tables.
///
/// This utilizes a few more locks than I'd like, however it should be good
/// enough for testing. In the future, modifications should be written to the
/// catalog tx then committed to the catalog at the end (without needing
/// interior mutability).
///
/// DDLs may seem unnecessarily complex right now with having to return a
/// "future" instead of just taking the lock and inserting th entry, but this is
/// exercising the functionality of executing DDL inside our scheduler. With
/// external data sources, these operations will be truly async, and so these
/// methods will make more sense then.
///
/// Actual storage is not transactional either.
#[derive(Debug)]
pub struct MemorySchema {
    inner: Arc<RwLock<MemorySchemaInner>>,
}

#[derive(Debug)]
struct MemorySchemaInner {
    // TODO: OIDs
    // TODO: Seperate maps for funcs/tables
    entries: HashMap<String, CatalogEntry>,
    tables: HashMap<String, MemoryDataTable>,
}

impl Schema for MemorySchema {
    fn try_get_entry(&self, _tx: &CatalogTx, name: &str) -> Result<Option<CatalogEntry>> {
        Ok(self.inner.read().entries.get(name).cloned())
    }

    fn get_data_table(&self, _tx: &CatalogTx, ent: &TableEntry) -> Result<Box<dyn DataTable>> {
        let table = self
            .inner
            .read()
            .tables
            .get(&ent.name)
            .cloned()
            .ok_or_else(|| {
                RayexecError::new(format!("Missing data table for entry: {}", ent.name))
            })?;

        Ok(Box::new(table) as _)
    }

    fn get_modifier(&self, _tx: &CatalogTx) -> Result<Box<dyn SchemaModifier>> {
        Ok(Box::new(MemorySchemaModifer {
            inner: self.inner.clone(),
        }) as _)
    }
}

#[derive(Debug)]
pub struct MemorySchemaModifer {
    inner: Arc<RwLock<MemorySchemaInner>>,
}

impl SchemaModifier for MemorySchemaModifer {
    fn create_table(&self, info: CreateTableInfo) -> Result<Box<dyn CreateFut>> {
        Ok(Box::new(MemoryCreateTable {
            info,
            inner: self.inner.clone(),
        }))
    }

    fn drop_table(&self, _name: &str) -> Result<Box<dyn DropFut>> {
        unimplemented!()
    }

    fn create_scalar_function(
        &self,
        _info: CreateScalarFunctionInfo,
    ) -> Result<Box<dyn CreateFut>> {
        unimplemented!()
    }

    fn create_aggregate_function(
        &self,
        _info: CreateScalarFunctionInfo,
    ) -> Result<Box<dyn CreateFut>> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct MemoryCreateTable {
    info: CreateTableInfo,
    inner: Arc<RwLock<MemorySchemaInner>>,
}

impl CreateFut for MemoryCreateTable {
    fn poll_create(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        let mut inner = self.inner.write();
        if inner.entries.contains_key(&self.info.name) {
            match self.info.on_conflict {
                OnConflict::Ignore => return Poll::Ready(Ok(())),
                OnConflict::Error => {
                    return Poll::Ready(Err(RayexecError::new(format!(
                        "Duplicate table name: {}",
                        self.info.name
                    ))))
                }
                OnConflict::Replace => (),
            }
        }

        inner.entries.insert(
            self.info.name.clone(),
            CatalogEntry::Table(TableEntry {
                name: self.info.name.clone(),
                columns: self.info.columns.clone(),
            }),
        );

        inner
            .tables
            .insert(self.info.name.clone(), MemoryDataTable::default());

        Poll::Ready(Ok(()))
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemoryDataTable {
    data: Arc<Mutex<Vec<Batch>>>,
}

impl DataTable for MemoryDataTable {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        let mut scans: Vec<_> = (0..num_partitions)
            .map(|_| MemoryDataTableScan { data: Vec::new() })
            .collect();

        let data = {
            let data = self.data.lock();
            data.clone()
        };

        for (idx, batch) in data.into_iter().enumerate() {
            scans[idx % num_partitions].data.push(batch);
        }

        Ok(scans
            .into_iter()
            .map(|scan| Box::new(scan) as Box<_>)
            .collect())
    }

    fn insert(&self, input_partitions: usize) -> Result<Vec<Box<dyn DataTableInsert>>> {
        let inserts: Vec<_> = (0..input_partitions)
            .map(|_| {
                Box::new(MemoryDataTableInsert {
                    collected: Vec::new(),
                    data: self.data.clone(),
                }) as _
            })
            .collect();

        Ok(inserts)
    }
}

#[derive(Debug)]
pub struct MemoryDataTableScan {
    data: Vec<Batch>,
}

impl DataTableScan for MemoryDataTableScan {
    fn poll_pull(&mut self, _cx: &mut Context) -> Result<PollPull> {
        match self.data.pop() {
            Some(batch) => Ok(PollPull::Batch(batch)),
            None => Ok(PollPull::Exhausted),
        }
    }
}

#[derive(Debug)]
pub struct MemoryDataTableInsert {
    collected: Vec<Batch>,
    data: Arc<Mutex<Vec<Batch>>>,
}

impl DataTableInsert for MemoryDataTableInsert {
    fn poll_push(&mut self, _cx: &mut Context, batch: Batch) -> Result<PollPush> {
        self.collected.push(batch);
        Ok(PollPush::Pushed)
    }

    fn finalize(&mut self) -> Result<()> {
        let mut data = self.data.lock();
        data.append(&mut self.collected);
        Ok(())
    }
}
