"""Finite exact filesystem identities for JSON metadata, separate from data.

Linux inode/device values may use uint64's high bit. Canonical decimal strings
preserve those values without changing the signed data/JSON integer contract.
"""
from demiflow.workbench.analysis import contracts as c


def unsigned(value,label):
    if type(value) is not int or not 0<=value<2**64:c.reject(label+" exceeds uint64 filesystem identity",code="resource_limit")
    return str(value)


def identity(info):
    if not info.st_ino:c.reject("Filesystem inode must be positive")
    return dict(device=unsigned(info.st_dev,"filesystem device"),inode=unsigned(info.st_ino,"filesystem inode"))


def parse(value):
    """Native comparison form; bounded legacy signed integers remain readable."""
    c.fields(value,{"device","inode"},label="filesystem identity")
    result={}
    for name,item in value.items():
        if type(item) is int:
            c.integer(item,"legacy filesystem "+name,2**63-1)
            result[name]=item;continue
        if (type(item) is not str or not 1<=len(item)<=20 or not item.isascii() or not item.isdecimal()
                or str(int(item))!=item or int(item)>=2**64):c.reject("Filesystem identity must be canonical bounded uint64")
        result[name]=int(item)
    if not result["inode"]:c.reject("Filesystem inode must be positive")
    return result
