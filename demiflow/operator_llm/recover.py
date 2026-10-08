"""Inspect/requeue selected uncertain calls; never submit a provider request.

python -m demiflow.operator_llm.recover inspect --path calls.sqlite
python -m demiflow.operator_llm.recover requeue --path calls.sqlite \
    --key SHA256 --actor NAME --reason TEXT --operation-id ID
"""
import argparse
import json
from pathlib import Path
from .call_ref import call_snapshot
from .sqlite_journal import SQLitePromptJournal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['inspect', 'requeue', 'history'])
    parser.add_argument('--path', required=True)
    parser.add_argument('--key', action='append')
    parser.add_argument('--actor')
    parser.add_argument('--reason')
    parser.add_argument('--operation-id')
    args = parser.parse_args()
    if not Path(args.path).is_file():
        parser.error('journal does not exist')
    if args.action == 'inspect':
        snapshot = call_snapshot(path=args.path)
        print(json.dumps({'responses': len(snapshot['responses']),
                          'uncertain': sorted(set(snapshot['requests']) - set(snapshot['responses']))}, indent=2))
        return
    journal = SQLitePromptJournal(args.path)
    try:
        if args.action == 'history':
            value = [{k: row[k] for k in ('operation_id', 'request_key', 'actor', 'reason', 'recovered_at', 'attempt')}
                     for row in journal.recovery_history()]
        else:
            if not args.key or not args.actor or not args.reason:
                parser.error('requeue requires --key, --actor and --reason')
            value = journal.requeue_uncertain(args.key, actor=args.actor, reason=args.reason, operation_id=args.operation_id)
        print(json.dumps(value, ensure_ascii=False, indent=2))
    finally:
        journal.close()


if __name__ == '__main__':
    main()
