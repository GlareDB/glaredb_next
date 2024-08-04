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

pub mod access {
    include!(concat!(env!("OUT_DIR"), "/rayexec.access.rs"));
}

pub mod binder {
    include!(concat!(env!("OUT_DIR"), "/rayexec.binder.rs"));
}

pub mod ast {
    include!(concat!(env!("OUT_DIR"), "/rayexec.ast.rs"));
}

pub mod logical {
    include!(concat!(env!("OUT_DIR"), "/rayexec.logical.rs"));
}
