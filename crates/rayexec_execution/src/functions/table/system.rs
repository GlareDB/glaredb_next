use std::{
    cell::RefCell, collections::VecDeque, fmt::Debug, future::Future, marker::PhantomData,
    sync::Arc,
};

use crate::database::{
    catalog::Catalog,
    table::{DataTable, DataTableScan, EmptyTableScan},
    DatabaseContext,
};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use rayexec_bullet::{
    array::{
        Array, BooleanArray, BooleanValuesBuffer, Utf8Array, ValuesBuffer, VarlenValuesBuffer,
    },
    batch::Batch,
    datatype::DataType,
    field::{Field, Schema},
};
use rayexec_error::{not_implemented, RayexecError, Result};

use super::{PlannedTableFunction, TableFunction, TableFunctionArgs};

trait SystemFunctionScan: Debug + Sync + Send {
    fn next_batch(&mut self) -> impl Future<Output = Result<Option<Batch>>> + Send + '_;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListCatalogs;

impl TableFunction for ListCatalogs {
    fn name(&self) -> &'static str {
        "list_catalogs"
    }

    fn plan_and_initialize(
        &self,
        context: &DatabaseContext,
        _args: TableFunctionArgs,
    ) -> BoxFuture<Result<Box<dyn PlannedTableFunction>>> {
        let func = ListCatalogsImpl {
            catalogs: context
                .iter_catalogs()
                .map(|(n, c)| (n.clone(), c.clone()))
                .collect(),
        };
        Box::pin(async move { Ok(Box::new(func) as _) })
    }

    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedTableFunction>> {
        not_implemented!("decoding system table functions")
    }
}

#[derive(Debug, Clone)]
pub struct ListCatalogsImpl {
    catalogs: VecDeque<(String, Arc<dyn Catalog>)>,
}

impl PlannedTableFunction for ListCatalogsImpl {
    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        not_implemented!("encoding system table functions")
    }

    fn table_function(&self) -> &dyn TableFunction {
        &ListCatalogs
    }

    fn schema(&self) -> Schema {
        Schema::new([
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("builtin", DataType::Boolean, false),
        ])
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(SystemDataTable::new(self.clone())))
    }
}

impl SystemFunctionScan for ListCatalogsImpl {
    fn next_batch(&mut self) -> impl Future<Output = Result<Option<Batch>>> + Send + '_ {
        async {
            if self.catalogs.is_empty() {
                return Ok(None);
            }

            let mut catalog_names = VarlenValuesBuffer::default();
            let mut builtin = BooleanValuesBuffer::default();

            while let Some((name, _catalog)) = self.catalogs.pop_front() {
                catalog_names.push_value(name);
                builtin.push_value(false);
            }

            let batch = Batch::try_new([
                Array::Utf8(Utf8Array::new(catalog_names, None)),
                Array::Boolean(BooleanArray::new(builtin, None)),
            ])?;

            Ok(Some(batch))
        }
    }
}

#[derive(Debug)]
struct SystemDataTable<S: SystemFunctionScan + 'static> {
    scan: Mutex<Option<S>>,
}

impl<S: SystemFunctionScan + 'static> SystemDataTable<S> {
    fn new(scan: S) -> Self {
        SystemDataTable {
            scan: Mutex::new(Some(scan)),
        }
    }
}

impl<S: SystemFunctionScan + 'static> DataTable for SystemDataTable<S> {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        let scan = self
            .scan
            .lock()
            .take()
            .ok_or_else(|| RayexecError::new("Scan called multiple times"))?;

        let mut scans: Vec<Box<dyn DataTableScan>> =
            vec![Box::new(SystemDataTableScan { scan }) as _];

        scans.extend((1..num_partitions).map(|_| Box::new(EmptyTableScan) as _));

        Ok(scans)
    }
}

#[derive(Debug)]
struct SystemDataTableScan<S: SystemFunctionScan> {
    scan: S,
}

impl<S: SystemFunctionScan> DataTableScan for SystemDataTableScan<S> {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(async { self.scan.next_batch().await })
    }
}
