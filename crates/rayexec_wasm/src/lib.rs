pub mod errors;

use errors::Result;
use rayexec_execution::engine::{session::Session, Engine, EngineRuntime};
use wasm_bindgen::prelude::*;

/// Wrapper around a database session and the engine that created it.
#[wasm_bindgen]
#[derive(Debug)]
pub struct WasmSession {
    engine: Engine,
    session: Session,
}

#[wasm_bindgen]
impl WasmSession {
    pub fn try_new() -> Result<WasmSession> {
        let runtime = EngineRuntime::try_new_shared()?;
        let engine = Engine::new(runtime)?;
        let session = engine.new_session()?;

        Ok(WasmSession { engine, session })
    }
}
