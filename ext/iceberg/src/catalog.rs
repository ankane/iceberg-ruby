#[cfg(feature = "datafusion")]
use datafusion::execution::context::SessionContext;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::Schema;
use iceberg::{Catalog, MemoryCatalog, NamespaceIdent, TableCreation, TableIdent};
#[cfg(feature = "glue")]
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
#[cfg(feature = "rest")]
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
#[cfg(feature = "sql")]
use iceberg_catalog_sql::{SqlBindStyle, SqlCatalog, SqlCatalogConfig};
#[cfg(feature = "datafusion")]
use iceberg_datafusion::IcebergCatalogProvider;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::to_rb_err;
use crate::runtime::runtime;
use crate::utils::Wrap;
use crate::{RbResult, RbTable};

pub enum RbCatalogType {
    #[cfg(feature = "glue")]
    Glue(Arc<GlueCatalog>),
    Memory(Arc<MemoryCatalog>),
    #[cfg(feature = "rest")]
    Rest(Arc<RestCatalog>),
    #[cfg(feature = "sql")]
    Sql(Arc<SqlCatalog>),
}

impl RbCatalogType {
    pub fn as_catalog(&self) -> &dyn Catalog {
        match self {
            #[cfg(feature = "glue")]
            RbCatalogType::Glue(v) => v.as_ref(),
            RbCatalogType::Memory(v) => v.as_ref(),
            #[cfg(feature = "rest")]
            RbCatalogType::Rest(v) => v.as_ref(),
            #[cfg(feature = "sql")]
            RbCatalogType::Sql(v) => v.as_ref(),
        }
    }

    #[cfg(feature = "datafusion")]
    fn as_arc(&self) -> Arc<dyn Catalog> {
        match self {
            #[cfg(feature = "glue")]
            RbCatalogType::Glue(v) => v.clone(),
            RbCatalogType::Memory(v) => v.clone(),
            #[cfg(feature = "rest")]
            RbCatalogType::Rest(v) => v.clone(),
            #[cfg(feature = "sql")]
            RbCatalogType::Sql(v) => v.clone(),
        }
    }
}

#[magnus::wrap(class = "Iceberg::RbCatalog")]
pub struct RbCatalog {
    pub catalog: RefCell<RbCatalogType>,
}

impl RbCatalog {
    #[cfg(feature = "glue")]
    pub fn new_glue(warehouse: String) -> RbResult<Self> {
        let config = GlueCatalogConfig::builder().warehouse(warehouse).build();
        let catalog = runtime()
            .block_on(GlueCatalog::new(config))
            .map_err(to_rb_err)?;
        Ok(Self {
            catalog: RbCatalogType::Glue(catalog.into()).into(),
        })
    }

    pub fn new_memory(warehouse: Option<String>) -> RbResult<Self> {
        let file_io = match warehouse {
            Some(ref v) => FileIO::from_path(v)
                .map_err(to_rb_err)?
                .build()
                .map_err(to_rb_err)?,
            None => FileIOBuilder::new_fs_io().build().map_err(to_rb_err)?,
        };
        let catalog = MemoryCatalog::new(file_io, warehouse);
        Ok(Self {
            catalog: RbCatalogType::Memory(catalog.into()).into(),
        })
    }

    #[cfg(feature = "rest")]
    pub fn new_rest(
        uri: String,
        warehouse: Option<String>,
        props: HashMap<String, String>,
    ) -> Self {
        let config = RestCatalogConfig::builder()
            .uri(uri)
            .warehouse_opt(warehouse)
            .props(props)
            .build();
        let catalog = RestCatalog::new(config);
        Self {
            catalog: RbCatalogType::Rest(catalog.into()).into(),
        }
    }

    #[cfg(feature = "sql")]
    pub fn new_sql(
        uri: String,
        warehouse: String,
        name: String,
        props: HashMap<String, String>,
    ) -> RbResult<Self> {
        let file_io = FileIO::from_path(&warehouse)
            .map_err(to_rb_err)?
            .build()
            .map_err(to_rb_err)?;
        let config = SqlCatalogConfig::builder()
            .uri(uri)
            .warehouse_location(warehouse)
            .name(name)
            .file_io(file_io)
            .sql_bind_style(SqlBindStyle::DollarNumeric)
            .props(props)
            .build();
        let catalog = runtime()
            .block_on(SqlCatalog::new(config))
            .map_err(to_rb_err)?;
        Ok(Self {
            catalog: RbCatalogType::Sql(catalog.into()).into(),
        })
    }

    pub fn list_namespaces(
        &self,
        parent: Option<Wrap<NamespaceIdent>>,
    ) -> RbResult<Vec<Vec<String>>> {
        let namespaces = runtime()
            .block_on(
                self.catalog
                    .borrow()
                    .as_catalog()
                    .list_namespaces(parent.map(|v| v.0).as_ref()),
            )
            .map_err(to_rb_err)?;
        Ok(namespaces.iter().map(|v| v.clone().inner()).collect())
    }

    pub fn create_namespace(
        &self,
        name: Wrap<NamespaceIdent>,
        props: HashMap<String, String>,
    ) -> RbResult<()> {
        runtime()
            .block_on(
                self.catalog
                    .borrow()
                    .as_catalog()
                    .create_namespace(&name.0, props),
            )
            .map_err(to_rb_err)?;
        Ok(())
    }

    pub fn namespace_exists(&self, name: Wrap<NamespaceIdent>) -> RbResult<bool> {
        let exists = runtime()
            .block_on(self.catalog.borrow().as_catalog().namespace_exists(&name.0))
            .map_err(to_rb_err)?;
        Ok(exists)
    }

    pub fn namespace_properties(
        &self,
        name: Wrap<NamespaceIdent>,
    ) -> RbResult<HashMap<String, String>> {
        let namespace = runtime()
            .block_on(self.catalog.borrow().as_catalog().get_namespace(&name.0))
            .map_err(to_rb_err)?;
        Ok(namespace.properties().clone())
    }

    pub fn update_namespace(
        &self,
        name: Wrap<NamespaceIdent>,
        props: HashMap<String, String>,
    ) -> RbResult<()> {
        runtime()
            .block_on(
                self.catalog
                    .borrow()
                    .as_catalog()
                    .update_namespace(&name.0, props),
            )
            .map_err(to_rb_err)?;
        Ok(())
    }

    pub fn drop_namespace(&self, name: Wrap<NamespaceIdent>) -> RbResult<()> {
        runtime()
            .block_on(self.catalog.borrow().as_catalog().drop_namespace(&name.0))
            .map_err(to_rb_err)?;
        Ok(())
    }

    pub fn list_tables(&self, namespace: Wrap<NamespaceIdent>) -> RbResult<Vec<Vec<String>>> {
        let tables = runtime()
            .block_on(self.catalog.borrow().as_catalog().list_tables(&namespace.0))
            .map_err(to_rb_err)?;
        Ok(tables
            .iter()
            .map(|v| {
                let mut vec = v.namespace.clone().inner();
                vec.push(v.name.clone());
                vec
            })
            .collect())
    }

    pub fn create_table(
        &self,
        name: Wrap<TableIdent>,
        schema: Wrap<Schema>,
        location: Option<String>,
    ) -> RbResult<RbTable> {
        let creation = TableCreation::builder()
            .name(name.0.name)
            .schema(schema.0)
            .location_opt(location)
            .build();
        let table = runtime()
            .block_on(
                self.catalog
                    .borrow()
                    .as_catalog()
                    .create_table(&name.0.namespace, creation),
            )
            .map_err(to_rb_err)?;
        Ok(RbTable {
            table: table.into(),
        })
    }

    pub fn load_table(&self, name: Wrap<TableIdent>) -> RbResult<RbTable> {
        let table = runtime()
            .block_on(self.catalog.borrow().as_catalog().load_table(&name.0))
            .map_err(to_rb_err)?;
        Ok(RbTable {
            table: table.into(),
        })
    }

    pub fn drop_table(&self, name: Wrap<TableIdent>) -> RbResult<()> {
        runtime()
            .block_on(self.catalog.borrow().as_catalog().drop_table(&name.0))
            .map_err(to_rb_err)?;
        Ok(())
    }

    pub fn table_exists(&self, name: Wrap<TableIdent>) -> RbResult<bool> {
        let exists = runtime()
            .block_on(self.catalog.borrow().as_catalog().table_exists(&name.0))
            .map_err(to_rb_err)?;
        Ok(exists)
    }

    pub fn rename_table(&self, name: Wrap<TableIdent>, new_name: Wrap<TableIdent>) -> RbResult<()> {
        runtime()
            .block_on(
                self.catalog
                    .borrow()
                    .as_catalog()
                    .rename_table(&name.0, &new_name.0),
            )
            .map_err(to_rb_err)?;
        Ok(())
    }

    pub fn register_table(
        &self,
        name: Wrap<TableIdent>,
        metadata_location: String,
    ) -> RbResult<()> {
        runtime()
            .block_on(
                self.catalog
                    .borrow()
                    .as_catalog()
                    .register_table(&name.0, metadata_location),
            )
            .map_err(to_rb_err)?;
        Ok(())
    }

    #[cfg(feature = "datafusion")]
    pub fn query(&self, sql: String) -> RbResult<()> {
        let runtime = runtime();

        // TODO only create context once
        let catalog = self.catalog.borrow().as_arc();
        let provider = runtime
            .block_on(IcebergCatalogProvider::try_new(catalog))
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_catalog("datafusion", Arc::new(provider));

        let df = runtime.block_on(ctx.sql(&sql)).unwrap();
        let _results = runtime.block_on(df.collect()).unwrap();

        // println!("{:?}", df.schema().fields());
        // println!("{:?}", results);

        Ok(())
    }
}
