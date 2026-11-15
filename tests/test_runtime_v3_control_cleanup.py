"""Physical Local cleanup closes control sockets even with retained tracebacks."""
from types import SimpleNamespace
import pytest
from demiflow import data
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext
from demiflow.resources.local import LocalResourceProvider


def _capture(monkeypatch):
    executions=[]
    prepare=DataflowExecution.prepare
    def captured(execution):
        executions.append(execution)  # Strong owners: garbage collection is not cleanup.
        return prepare(execution)
    monkeypatch.setattr(DataflowExecution,'prepare',captured)
    return executions


def test_repeated_real_failure_releases_driver_sockets_with_tracebacks_alive(tmp_path,monkeypatch):
    executions=_capture(monkeypatch);errors=[]
    for number in range(3):
        try:
            with RunContext.start(tmp_path/str(number)),data.local_execution():
                data.from_items([{'id':3}]).prepend(data.from_items([{'id':1},{'id':2}]),max_rows=1).take_all()
        except Exception as exc:
            assert 'max_rows' in str(exc);errors.append(exc)
        else:pytest.fail('oversized prefix was accepted')
        connector=executions[-1].services.resources.provider._control
        assert connector.driver_socks=={}
        assert connector._pending=={} and connector._staged=={}
    assert len(errors)==3 and all(error.__traceback__ is not None for error in errors)


def test_real_phase_retirement_and_stream_terminal_close_owned_transports(tmp_path,monkeypatch):
    from test_runtime_v3_residency import LIMITS,dataset
    import pyarrow as pa
    executions=_capture(monkeypatch);launches=[]
    launch=DataflowExecution._launch_phase
    def launched(execution):
        if launches:
            assert not execution.services.resources.provider._control.driver_socks
        result=launch(execution);launches.append(execution._phase_index)
        return result
    monkeypatch.setattr(DataflowExecution,'_launch_phase',launched)
    with RunContext.start(tmp_path/'phases',limits=LIMITS),data.local_execution():
        assert dataset().take_all()==[{'n':n} for n in range(1,5)]
    assert len(launches)>=3
    assert executions[-1].services.resources.provider._control.driver_socks=={}
    launches.clear()
    with RunContext.start(tmp_path/'native'),data.local_execution():
        result=data.from_items([{'id':1}]).save_lance(tmp_path/'saved.lance',schema=pa.schema([('id',pa.int64())]),
            key='id',stage='saved').run_stream()
        assert result.outputs['saved']['version']==1
    assert executions[-1].services.resources.provider._control.driver_socks=={}


@pytest.mark.parametrize('phase,confirmed',[('REQUESTED',True),('RUNNING',True),('RUNNING',False)])
def test_control_cleanup_cannot_fabricate_exit_or_erase_confirmed_exit(phase,confirmed,caplog):
    released=[];closed=[]
    pool=SimpleNamespace(reservation_view=lambda allocation:None,release=lambda *args:released.append(args))
    unit=SimpleNamespace(inspect=lambda *args,**kwargs:{'phase':phase},
        stop=lambda *args,**kwargs:{'confirmed':confirmed,'identity':None,'evidence_ref':'actual-proof'})
    def close(allocation):
        closed.append(allocation)
        raise OSError('test local socket close failed')
    provider=LocalResourceProvider(pool,unit,control=SimpleNamespace(close=close))
    result=provider.ensure_absent({'scope_id':'owned-scope','allocation_id':'owned-allocation'},100)
    assert result['cleanup']==('CONFIRMED' if confirmed else 'UNCONFIRMED')
    assert closed==(['owned-allocation'] if confirmed else [])
    assert bool(released)==(confirmed and phase=='RUNNING')
    if confirmed:assert 'socket close failed' in caplog.text
