use futures::future::BoxFuture;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{Result, ResultExt};
use rayexec_execution::{
    database::{
        catalog::{Catalog, CatalogTx},
        entry::TableEntry,
        table::{DataTable, DataTableScan},
    },
    datasource::{check_options_empty, take_option, DataSource},
};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
pub struct PostgresDataSource;

impl DataSource for PostgresDataSource {
    fn create_catalog(
        &self,
        mut options: HashMap<String, OwnedScalarValue>,
    ) -> Result<Box<dyn Catalog>> {
        let conn_str = take_option("connection_string", &mut options)?.try_into_string()?;
        check_options_empty(&options)?;

        // Check we can connect.
        // let _client =
        //     Client::connect(&conn_str, NoTls).context("Failed to connect to postgres instance")?;

        Ok(Box::new(PostgresCatalog { conn_str }))
    }
}

#[derive(Debug)]
pub struct PostgresCatalog {
    // TODO: Connection pooling.
    conn_str: String,
}

impl Catalog for PostgresCatalog {
    fn get_table_entry(
        &self,
        tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> BoxFuture<Result<Option<TableEntry>>> {
        unimplemented!()
    }

    fn data_table(
        &self,
        tx: &CatalogTx,
        schema: &str,
        ent: &TableEntry,
    ) -> Result<Box<dyn DataTable>> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct PostgresDataTable {}

impl DataTable for PostgresDataTable {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        unimplemented!()
    }
}
