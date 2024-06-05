use futures::future::BoxFuture;
use parking_lot::{Mutex, RwLock};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::database::catalog::{Catalog, CatalogTx};
use crate::database::create::{
    CreateScalarFunctionInfo, CreateSchemaInfo, CreateTableInfo, OnConflict,
};
use crate::database::ddl::{CatalogModifier, CreateFut, DropFut};
use crate::database::drop::{DropInfo, DropObject};
use crate::database::entry::{CatalogEntry, TableEntry};
use crate::{
    database::table::{DataTable, DataTableInsert, DataTableScan},
    execution::operators::{PollPull, PollPush},
};

/// Quick and dirty in-memory implementation of a catalog and related data
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
pub struct MemoryCatalog {
    inner: Arc<RwLock<MemorySchemas>>,
}

#[derive(Debug)]
struct MemorySchemas {
    schemas: HashMap<String, MemorySchema>,
}

#[derive(Debug, Default)]
struct MemorySchema {
    // TODO: OIDs
    // TODO: Seperate maps for funcs/tables
    entries: HashMap<String, CatalogEntry>,
    tables: HashMap<String, MemoryDataTable>,
}

impl MemoryCatalog {
    /// Creates a new memory catalog with a single named schema.
    pub fn new_with_schema(schema: &str) -> Self {
        let mut schemas = HashMap::new();
        schemas.insert(schema.to_string(), MemorySchema::default());

        MemoryCatalog {
            inner: Arc::new(RwLock::new(MemorySchemas { schemas })),
        }
    }

    fn get_table_entry_inner(
        &self,
        _tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<TableEntry>> {
        let inner = self.inner.read();
        let schema = inner
            .schemas
            .get(schema)
            .ok_or_else(|| RayexecError::new(format!("Missing schema: {schema}")))?;

        match schema.entries.get(name) {
            Some(CatalogEntry::Table(ent)) => Ok(Some(ent.clone())),
            Some(_) => Err(RayexecError::new("Entry not a table")),
            None => Ok(None),
        }
    }
}

impl Catalog for MemoryCatalog {
    fn get_table_entry(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> BoxFuture<Result<Option<TableEntry>>> {
        let result = self.get_table_entry_inner(tx, schema, name);
        Box::pin(async { result })
    }

    fn data_table(
        &self,
        _tx: &CatalogTx,
        schema: &str,
        ent: &TableEntry,
    ) -> Result<Box<dyn DataTable>> {
        let inner = self.inner.read();
        let schema = inner
            .schemas
            .get(schema)
            .ok_or_else(|| RayexecError::new(format!("Missing schema: {schema}")))?;
        let table = schema
            .tables
            .get(&ent.name)
            .cloned()
            .ok_or_else(|| RayexecError::new(format!("Missing table: {}", ent.name)))?;

        Ok(Box::new(table) as _)
    }

    fn catalog_modifier(&self, _tx: &CatalogTx) -> Result<Box<dyn CatalogModifier>> {
        Ok(Box::new(MemoryCatalogModifier {
            inner: self.inner.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct MemoryCatalogModifier {
    inner: Arc<RwLock<MemorySchemas>>,
}

impl CatalogModifier for MemoryCatalogModifier {
    fn create_schema(&self, create: CreateSchemaInfo) -> Result<Box<dyn CreateFut<Output = ()>>> {
        Ok(Box::new(MemoryCreateSchema {
            schema: create.name.to_string(),
            on_conflict: create.on_conflict,
            inner: self.inner.clone(),
        }))
    }

    fn create_table(
        &self,
        schema: &str,
        info: CreateTableInfo,
    ) -> Result<Box<dyn CreateFut<Output = Box<dyn DataTable>>>> {
        Ok(Box::new(MemoryCreateTable {
            schema: schema.to_string(),
            info,
            inner: self.inner.clone(),
        }))
    }

    fn create_scalar_function(
        &self,
        _info: CreateScalarFunctionInfo,
    ) -> Result<Box<dyn CreateFut<Output = ()>>> {
        unimplemented!()
    }

    fn create_aggregate_function(
        &self,
        _info: CreateScalarFunctionInfo,
    ) -> Result<Box<dyn CreateFut<Output = ()>>> {
        unimplemented!()
    }

    fn drop_entry(&self, drop: DropInfo) -> Result<Box<dyn DropFut>> {
        Ok(Box::new(MemoryDrop {
            info: drop,
            inner: self.inner.clone(),
        }))
    }
}

#[derive(Debug)]
struct MemoryCreateSchema {
    schema: String,
    on_conflict: OnConflict,
    inner: Arc<RwLock<MemorySchemas>>,
}

impl CreateFut for MemoryCreateSchema {
    type Output = ();
    fn poll_create(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        let mut inner = self.inner.write();

        let schema_exists = inner.schemas.contains_key(&self.schema);
        match self.on_conflict {
            OnConflict::Ignore if schema_exists => return Poll::Ready(Ok(())),
            OnConflict::Error if schema_exists => {
                return Poll::Ready(Err(RayexecError::new(format!(
                    "Schema already exists: {}",
                    self.schema
                ))))
            }
            OnConflict::Replace => {
                return Poll::Ready(Err(RayexecError::new("Cannot replace schema")))
            }
            _ => (), // Otherwise continue with the create.
        }

        inner
            .schemas
            .insert(self.schema.clone(), MemorySchema::default());

        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
struct MemoryDrop {
    info: DropInfo,
    inner: Arc<RwLock<MemorySchemas>>,
}

impl DropFut for MemoryDrop {
    fn poll_drop(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        let mut inner = self.inner.write();

        if self.info.cascade {
            return Poll::Ready(Err(RayexecError::new("DROP CASCADE not implemented")));
        }

        match &self.info.object {
            DropObject::Schema => {
                if !self.info.if_exists && !inner.schemas.contains_key(&self.info.schema) {
                    return Poll::Ready(Err(RayexecError::new(format!(
                        "Missing schema: {}",
                        self.info.schema
                    ))));
                }

                inner.schemas.remove(&self.info.schema);

                Poll::Ready(Ok(()))
            }
            other => Poll::Ready(Err(RayexecError::new(format!(
                "Drop unimplemted for object: {other:?}"
            )))),
        }
    }
}

#[derive(Debug)]
struct MemoryCreateTable {
    schema: String,
    info: CreateTableInfo,
    inner: Arc<RwLock<MemorySchemas>>,
}

impl CreateFut for MemoryCreateTable {
    type Output = Box<dyn DataTable>;
    fn poll_create(&mut self, _cx: &mut Context) -> Poll<Result<Self::Output>> {
        let mut inner = self.inner.write();
        let schema = match inner.schemas.get_mut(&self.schema) {
            Some(schema) => schema,
            None => {
                return Poll::Ready(Err(RayexecError::new(format!(
                    "Missing schema: {}",
                    &self.schema
                ))))
            }
        };
        if schema.entries.contains_key(&self.info.name) {
            match self.info.on_conflict {
                OnConflict::Ignore => unimplemented!(), // TODO: What to do here?
                OnConflict::Error => {
                    return Poll::Ready(Err(RayexecError::new(format!(
                        "Duplicate table name: {}",
                        self.info.name
                    ))))
                }
                OnConflict::Replace => (),
            }
        }

        schema.entries.insert(
            self.info.name.clone(),
            CatalogEntry::Table(TableEntry {
                name: self.info.name.clone(),
                columns: self.info.columns.clone(),
            }),
        );

        let table = MemoryDataTable::default();
        schema.tables.insert(self.info.name.clone(), table.clone());

        Poll::Ready(Ok(Box::new(table)))
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
        Ok(PollPush::NeedsMore)
    }

    fn finalize(&mut self) -> Result<()> {
        let mut data = self.data.lock();
        data.append(&mut self.collected);
        Ok(())
    }
}
