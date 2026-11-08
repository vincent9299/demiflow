"""Stat inode values are uint64; schema validity never proves cleanup."""
import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.local_read_retirement import _identity
from demiflow.workbench.analysis.local_read_retirement import _inode_token
from demiflow.forge.schema import ForgeError


@pytest.mark.parametrize('inode',[2**63-1,2**63,9367487225521426887,2**64-1])
def test_private_directory_inode_preserves_uint64_without_relaxing_common_json(inode):
    #9367487225521426887 is the actual I88 control-directory stat value.
    identity=dict(device=196,inode=_inode_token(inode))
    _identity(identity)
    assert c.decode(c.encoded(identity))==identity
    assert int(identity['inode'])==inode
    if inode>=2**63:
        with pytest.raises(ForgeError):c.encoded(dict(inode=inode))


@pytest.mark.parametrize('inode',[True,0,-1,2**64,1.5,'01','1','9223372036854775807','18446744073709551616'])
def test_out_of_range_or_untyped_inode_remains_refused(inode):
    with pytest.raises(ForgeError):_identity(dict(device=196,inode=inode))
