pub mod errors;
pub mod shell;
pub mod tracing;

use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen]
pub fn init_panic_handler() {
    console_error_panic_hook::set_once();
}
