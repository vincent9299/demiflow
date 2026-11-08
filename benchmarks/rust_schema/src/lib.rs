//! Benchmark-only accepting predicate. Python remains the validation authority.
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};

const MAX_NODES: usize = 512;
const MAX_DEPTH: usize = 12;
const MAX_VISITS: usize = 65_536;

fn unsupported() -> PyErr {
    PyValueError::new_err("schema is outside the bounded benchmark subset")
}

enum Kind {
    Object { fields: Vec<(Py<PyString>, Node, bool)>, keys: Py<PyDict>, extra: bool },
    Array(Box<Node>),
    String,
    Integer,
    Number,
    Boolean,
    Null,
}

struct Node {
    kind: Kind,
    low_len: Option<usize>,
    high_len: Option<usize>,
    bounds: Vec<(&'static str, Py<PyAny>)>,
    constant: Option<Py<PyString>>,
    choices: Option<Vec<Py<PyString>>>,
}

fn string(value: &Bound<'_, PyAny>) -> PyResult<Py<PyString>> {
    Ok(value.cast_exact::<PyString>().map_err(|_| unsupported())?.clone().unbind())
}

fn length(schema: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<usize>> {
    schema.get_item(key)?.map(|v| {
        v.cast_exact::<PyInt>().map_err(|_| unsupported())?;
        v.extract::<usize>().map_err(|_| unsupported())
    }).transpose()
}

impl Node {
    fn compile(schema: &Bound<'_, PyAny>, depth: usize, nodes: &mut usize) -> PyResult<Self> {
        *nodes += 1;
        if depth > MAX_DEPTH || *nodes > MAX_NODES { return Err(unsupported()); }
        let s = schema.cast_exact::<PyDict>().map_err(|_| unsupported())?;
        let raw_name = s.get_item("type")?.ok_or_else(unsupported)?;
        let raw_name = raw_name.cast_exact::<PyString>().map_err(|_| unsupported())?;
        if raw_name.len()? > 16 { return Err(unsupported()); }
        let name: String = raw_name.extract().map_err(|_| unsupported())?;
        // Conservatively reject keywords whose general Python equality or date
        // semantics would need another implementation. The wrapper falls back.
        let allowed = match name.as_str() {
            "object" => vec!["type", "properties", "required", "additionalProperties"],
            "array" => vec!["type", "items", "minItems", "maxItems"],
            "string" => vec!["type", "minLength", "maxLength", "const", "enum"],
            "integer" | "number" => vec!["type", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"],
            "boolean" | "null" => vec!["type"],
            _ => return Err(unsupported()),
        };
        for (key, _) in s.iter() {
            let key = key.cast_exact::<PyString>().map_err(|_| unsupported())?;
            if key.len()? > 24 { return Err(unsupported()); }
            let key: String = key.extract().map_err(|_| unsupported())?;
            if !allowed.contains(&key.as_str()) { return Err(unsupported()); }
        }
        let py = schema.py();
        let kind = match name.as_str() {
            "object" => {
                let props = s.get_item("properties")?.unwrap_or_else(|| PyDict::new(py).into_any());
                let props = props.cast_exact::<PyDict>().map_err(|_| unsupported())?;
                if props.len() > MAX_NODES { return Err(unsupported()); }
                let required = s.get_item("required")?.unwrap_or_else(|| PyList::empty(py).into_any());
                let required = required.cast_exact::<PyList>().map_err(|_| unsupported())?;
                if required.len() > MAX_NODES { return Err(unsupported()); }
                for r in required.iter() {
                    r.cast_exact::<PyString>().map_err(|_| unsupported())?;
                    if !props.contains(&r)? { return Err(unsupported()); }
                }
                let keys = PyDict::new(py);
                let mut fields = Vec::with_capacity(props.len());
                for (key, child) in props.iter() {
                    let owned = string(&key)?;
                    let need = required.contains(&key)?;
                    keys.set_item(&key, true)?;
                    fields.push((owned, Self::compile(&child, depth + 1, nodes)?, need));
                }
                let extra = match s.get_item("additionalProperties")? {
                    None => false,
                    Some(v) => v.cast_exact::<PyBool>().map_err(|_| unsupported())?.extract::<bool>()?,
                };
                Kind::Object { fields, keys: keys.unbind(), extra }
            },
            "array" => Kind::Array(Box::new(Self::compile(&s.get_item("items")?.ok_or_else(unsupported)?, depth + 1, nodes)?)),
            "string" => Kind::String,
            "integer" => Kind::Integer,
            "number" => Kind::Number,
            "boolean" => Kind::Boolean,
            "null" => Kind::Null,
            _ => return Err(unsupported()),
        };
        let (low, high) = if name == "array" { ("minItems", "maxItems") } else { ("minLength", "maxLength") };
        let constant = s.get_item("const")?.map(|v| string(&v)).transpose()?;
        let choices = s.get_item("enum")?.map(|v| {
            let list = v.cast_exact::<PyList>().map_err(|_| unsupported())?;
            if list.is_empty() || list.len() > MAX_NODES { return Err(unsupported()); }
            list.iter().map(|v| string(&v)).collect::<PyResult<Vec<_>>>()
        }).transpose()?;
        let mut bounds = Vec::new();
        for key in ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"] {
            if let Some(v) = s.get_item(key)? {
                if !(v.is_exact_instance_of::<PyInt>() || v.is_exact_instance_of::<PyFloat>()) { return Err(unsupported()); }
                bounds.push((key, v.unbind()));
            }
        }
        Ok(Self { kind, low_len: length(s, low)?, high_len: length(s, high)?, bounds, constant, choices })
    }

    fn accepts(&self, value: &Bound<'_, PyAny>, remaining: &mut usize) -> PyResult<bool> {
        if *remaining == 0 { return Ok(false); }
        *remaining -= 1;
        let py = value.py();
        let valid = match &self.kind {
            Kind::Object { fields, keys, extra } => {
                let Ok(v) = value.cast_exact::<PyDict>() else { return Ok(false); };
                if v.len() > *remaining { return Ok(false); }
                *remaining -= v.len();
                for (key, _) in v.iter() {
                    if !key.is_exact_instance_of::<PyString>() || (!extra && !keys.bind(py).contains(&key)?) { return Ok(false); }
                }
                for (key, node, required) in fields {
                    match v.get_item(key.bind(py))? {
                        Some(child) => if !node.accepts(&child, remaining)? { return Ok(false); },
                        None => if *required { return Ok(false); },
                    }
                }
                true
            },
            Kind::Array(node) => {
                let Ok(v) = value.cast_exact::<PyList>() else { return Ok(false); };
                if v.len() > *remaining || !self.valid_len(v.len()) { return Ok(false); }
                for item in v.iter() { if !node.accepts(&item, remaining)? { return Ok(false); } }
                true
            },
            Kind::String => {
                let Ok(v) = value.cast_exact::<PyString>() else { return Ok(false); };
                // Python len counts Unicode code points, including lone surrogates.
                self.valid_len(v.len()?)
            },
            Kind::Integer => value.is_exact_instance_of::<PyInt>(),
            Kind::Number => value.is_exact_instance_of::<PyInt>() || value.is_exact_instance_of::<PyFloat>(),
            Kind::Boolean => value.is_exact_instance_of::<PyBool>(),
            Kind::Null => value.is_none(),
        };
        if !valid { return Ok(false); }
        if let Some(c) = &self.constant { if !value.eq(c.bind(py))? { return Ok(false); } }
        if let Some(choices) = &self.choices {
            let mut found = false;
            for item in choices { if value.eq(item.bind(py))? { found = true; break; } }
            if !found { return Ok(false); }
        }
        // Never cast arbitrary Python integers to f64: large integer bounds,
        // bool exclusion and NaN comparison behavior must remain unchanged.
        for (key, bound) in &self.bounds {
            let bound = bound.bind(py);
            let invalid = match *key {
                "minimum" => value.lt(bound)?,
                "maximum" => value.gt(bound)?,
                "exclusiveMinimum" => value.le(bound)?,
                "exclusiveMaximum" => value.ge(bound)?,
                _ => unreachable!(),
            };
            if invalid { return Ok(false); }
        }
        Ok(true)
    }

    fn valid_len(&self, len: usize) -> bool {
        self.low_len.is_none_or(|v| len >= v) && self.high_len.is_none_or(|v| len <= v)
    }
}

#[pyclass(frozen)]
struct Predicate { root: Node }

#[pymethods]
impl Predicate {
    #[new]
    fn new(schema: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self { root: Node::compile(schema, 0, &mut 0)? })
    }

    fn accepts(&self, value: &Bound<'_, PyAny>) -> bool {
        // Predicate failure is never an acceptance; the Python wrapper performs
        // authoritative validation and produces the original diagnostic.
        let mut remaining = MAX_VISITS;
        self.root.accepts(value, &mut remaining).unwrap_or(false)
    }
}

#[pymodule]
fn _demiflow_schema_probe(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Predicate>()
}
