//! Included generated code.

pub mod schema {
    include!(concat!(env!("OUT_DIR"), "/rayexec.schema.rs"));
}

pub mod array {
    include!(concat!(env!("OUT_DIR"), "/rayexec.array.rs"));
}

pub mod execution {
    include!(concat!(env!("OUT_DIR"), "/rayexec.execution.rs"));
}

pub mod expr {
    include!(concat!(env!("OUT_DIR"), "/rayexec.expr.rs"));
}
