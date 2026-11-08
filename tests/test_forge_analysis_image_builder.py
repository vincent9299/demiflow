"""Image assembly guards; no deployment, container or image execution."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).parents[1]/"tools/forge_analysis/build_image.py"
spec = importlib.util.spec_from_file_location("forge_analysis_image_builder", SCRIPT)
builder = importlib.util.module_from_spec(spec); spec.loader.exec_module(builder)


def test_image_paths_and_conflicting_inputs_never_overwrite_existing_content(tmp_path):
    output = builder.Builder(tmp_path/"out")
    first=tmp_path/"first"; first.write_bytes(b"first")
    second=tmp_path/"second"; second.write_bytes(b"second")
    output.add(first, "usr/local/first", provenance="fixture")
    with pytest.raises(ValueError, match="Conflicting"):
        output.add(second, "usr/local/first", provenance="fixture")
    assert (output.root/"usr/local/first").read_bytes() == b"first"
    for path in ("../outside", "/etc/x", "a/../../x", "a//b", "a/./b"):
        with pytest.raises(ValueError):
            output.add(second, path, provenance="fixture")
    assert not (tmp_path/"outside").exists()


def test_image_byte_reservation_rejects_before_copying_the_next_file(tmp_path, monkeypatch):
    output=builder.Builder(tmp_path/"out")
    source=tmp_path/"source"; source.write_bytes(b"12345")
    monkeypatch.setattr(builder, "MAX_BYTES", 4)
    with pytest.raises(ValueError, match="reservation exhausted"):
        output.add(source, "file", provenance="fixture")
    assert not list(output.root.iterdir())


def test_stdlib_copy_excludes_personal_startup_hooks_and_unregistered_files(tmp_path, monkeypatch):
    interpreter=tmp_path/"python"; interpreter.write_bytes(b"fixed interpreter fixture")
    library=tmp_path/"stdlib"; (library/"encodings").mkdir(parents=True)
    (library/"os.py").write_text("# standard module")
    (library/"encodings/__init__.py").write_text("# encodings")
    (library/"sitecustomize.py").write_text("raise RuntimeError('must not enter image')")
    (library/"unexpected-private-file").write_text("private fixture")
    (library/"site-packages").mkdir(); (library/"site-packages/unknown.pth").write_text("import private_hook")
    monkeypatch.setattr(builder,"sys",SimpleNamespace(executable=str(interpreter),stdlib_module_names={"os","encodings"}))
    monkeypatch.setattr(builder,"sysconfig",SimpleNamespace(get_path=lambda _:str(library)))
    output=builder.Builder(tmp_path/"out"); output.interpreter()
    assert (output.root/"usr/local/lib/python3.11/os.py").is_file()
    assert (output.root/"usr/local/lib/python3.11/encodings/__init__.py").is_file()
    assert not any(name.endswith("sitecustomize.py") or "private-file" in name or ".pth" in name for name in output.files)
