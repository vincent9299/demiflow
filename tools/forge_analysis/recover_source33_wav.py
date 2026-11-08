"""One reviewed Source33 legacy WAV maintenance target; never a public API.

Preparation runs before the installed Source33 closure is replaced. It does
not start/stop services, update packages, or edit an owner/lease/status.
"""
import argparse
from pathlib import Path
import sys

EXPECTED = {'state': '/yzp/zhaozy/yangzepeng/0905/.df/wbe719c5', 'job_id': 'j60d85e2c3dc746bb96800be7d19d2786', 'read_request_id': 'rbc3f34984bd044c3a58523f0b426e340', 'context_sha256': 'c7f123c522e17ae04993d16efa9fdc0d0094d92a4e56987b53c46f5bacd44fe1', 'owner_sha256': '3a4355b2c674babdf447929ba6934b97f1574a9a21a907acdcfc2d653f6fb8c8', 'origin_digest': 'b69bc9a11fced796630facf9156abd2b0e1ec2d35ee2b0c7eb2ef16618cebae0', 'unit_identity': {'device': 196, 'inode': 144115188541490409}, 'artifact': {'bytes': 24395, 'digest': '9e3dee43dcf6560f4472b3d31cd8a0c4e18684dea77df0c30c827659287f6010', 'generation': 1, 'id': 'o0597f03dc0ec498ca949e98bfae3105c', 'sha256': '3b1026369bf01b927fea3a37fa542a8886c6a9368dfedbf88d37e3346261e780'}, 'source_manifest': {'path': '/yzp/zhaozy/yangzepeng/0905/_demiflow/forge_coordination_20261104/workflow_workbench_20261105/P2/integration-source-input-33/manifest.json', 'sha256': '1ddf5f2789981a2630973bc10d5eb4029d42192c82e3b574034a4f164b00b181'}, 'installation_receipt': {'path': '/yzp/zhaozy/yangzepeng/0905/_demiflow/forge_coordination_20261104/parallel_20261106_1705/runtime_deploy/evidence/source33-installation-01/installation-receipt.json', 'sha256': '45580986172eb800049f0c9d60001c0719b88e46852c1d89abf56d4febc95301'}, 'controller_lock_identity': {'device': 196, 'inode': 144115188541387159}, 'proof_scope': 'reviewed_closed_wav_unit_domain', 'filesystem_test': {'path': '/yzp/zhaozy/yangzepeng/0905/_demiflow/forge_coordination_20261104/parallel_20261106_1705/runtime_deploy/evidence/source33-startup-cancel-02/same-device-attestation.json', 'sha256': '4494b906e6d8351e4097925c38d86bbccb4be30d7f2f45fb967db90e3897b469'}, 'original_lease_snapshot': {'path': '/yzp/zhaozy/yangzepeng/0905/_demiflow/forge_coordination_20261104/parallel_20261106_1705/runtime_deploy/evidence/source33-startup-cancel-02/intake/original-public-lease.json', 'sha256': '999565207885d7c9ff1c2c55b81fc67ff03a157e5def0807619e84ce5a71cf6c'}}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--code-root',required=True)
    parser.add_argument('--authorization',required=True)
    parser.add_argument('--authorization-sha256',required=True)
    parser.add_argument('--snapshot',required=True)
    args=parser.parse_args()
    root=Path(args.code_root)
    if not root.is_absolute() or root.is_symlink() or '..' in root.parts:
        raise ValueError('Fixed reviewed maintenance code root required')
    sys.path.insert(0,str(root))
    from demiflow.workbench.analysis import legacy_wav_recovery as recovery
    auth,_=recovery.authorization(Path(args.authorization),args.authorization_sha256)
    if any(auth[key]!=value for key,value in EXPECTED.items()):
        raise ValueError('This maintenance entry only accepts the reviewed original Source33 target')
    witness=recovery.publish(Path(auth['state'])/'runtime',Path(args.authorization),
        args.authorization_sha256,Path(args.snapshot))
    print(__import__('json').dumps(dict(prepared=True,job_id=witness['job_id'],
        status_created=False,lease_or_owner_edited=False,public_service_actions=False)))


if __name__=='__main__':main()
