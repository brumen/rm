use pyo3::prelude::*;
use sabr_calib::calibrate_sabr_to_vols;

#[pyfunction]
fn calibrate_sabr_to_vols_py() -> PyResult<String> {
    // Call the Rust function and handle the result
    let result = calibrate_sabr_to_vols();
    Ok(result)
}

#[pymodule]
fn sabr_calib(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(calibrate_sabr_to_vols_py, m)?)?;
    Ok(())
}
use pyo3::prelude::*;
use sabr_calib::calibrate_sabr_to_vols;

#[pyfunction]
fn calibrate_sabr_to_vols_py() -> PyResult<String> {
    // Call the Rust function and handle the result
    let result = calibrate_sabr_to_vols();
    Ok(result)
}

#[pymodule]
fn sabr_calib(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(calibrate_sabr_to_vols_py, m)?)?;
    Ok(())
}
