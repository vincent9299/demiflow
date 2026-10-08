"""Read Brave's Svelte data literals without executing page JavaScript.

Input <=8 MiB, nesting <=64, <=200k values, <=4096 scalar function arguments.
Only literal objects/arrays and Svelte's scalar-substitution IIFEs are accepted;
arbitrary calls, property access and expressions fail closed.
"""
import json
import re
from dataclasses import dataclass

from lxml import html


@dataclass(frozen=True)
class Reference:
    name: str


class LiteralReader:
    def __init__(self, text, start=0):
        self.text=text;self.pos=start;self.nodes=0
        self.decoder=json.JSONDecoder()

    def space(self):
        while self.pos<len(self.text) and self.text[self.pos].isspace():self.pos+=1

    def token(self, text):
        self.space()
        if not self.text.startswith(text,self.pos):raise ValueError('Unsupported Brave data syntax')
        self.pos+=len(text)

    def identifier(self):
        self.space()
        match=re.match(r'[A-Za-z_$][A-Za-z0-9_$]*',self.text[self.pos:self.pos+256])
        if not match:raise ValueError('Invalid Brave data identifier')
        self.pos+=len(match[0]);return match[0]

    def value(self, depth=0):
        self.nodes+=1
        if depth>64 or self.nodes>200000:raise ValueError('Brave data resource limit')
        self.space()
        char=self.text[self.pos:self.pos+1]
        if char=='{':
            self.pos+=1;result={};self.space()
            while not self.text.startswith('}',self.pos):
                self.space()
                key=self.value(depth+1) if self.text[self.pos:self.pos+1]=='"' else self.identifier()
                if not isinstance(key,str) or key in result:raise ValueError('Invalid Brave object key')
                self.token(':');result[key]=self.value(depth+1);self.space()
                if self.text.startswith('}',self.pos):break
                self.token(',');self.space()
            self.token('}');return result
        if char=='[':
            self.pos+=1;result=[];self.space()
            while not self.text.startswith(']',self.pos):
                result.append(self.value(depth+1));self.space()
                if self.text.startswith(']',self.pos):break
                self.token(',');self.space()
            self.token(']');return result
        if char=='(':
            self.token('(');self.token('function');self.token('(')
            names=[];self.space()
            while not self.text.startswith(')',self.pos):
                if len(names)>=4096:raise ValueError('Brave function argument limit')
                names.append(self.identifier());self.space()
                if self.text.startswith(')',self.pos):break
                self.token(',')
            if len(set(names))!=len(names):raise ValueError('Duplicate Brave function arguments')
            self.token(')');self.token('{');self.token('return')
            body=self.value(depth+1);self.space()
            if self.text.startswith(';',self.pos):self.pos+=1
            self.token('}');self.token('(')
            args=[];self.space()
            while not self.text.startswith(')',self.pos):
                if len(args)>=len(names):raise ValueError('Brave function argument mismatch')
                arg=self.value(depth+1)
                if isinstance(arg,(dict,list,Reference)):raise ValueError('Brave arguments must be scalar literals')
                args.append(arg);self.space()
                if self.text.startswith(')',self.pos):break
                self.token(',')
            self.token(')');self.token(')')
            if len(args)!=len(names):raise ValueError('Brave function argument mismatch')
            return self.bind(body,dict(zip(names,args)),depth+1)
        if char=='"' or (char and char in '-0123456789'):
            value,end=self.decoder.raw_decode(self.text,self.pos);self.pos=end;return value
        name=self.identifier()
        if name in ('true','false','null','undefined'):
            return {'true':True,'false':False,'null':None,'undefined':None}[name]
        if name=='void':self.token('0');return None
        return Reference(name)

    def bind(self, value, arguments, depth):
        self.nodes+=1
        if depth>64 or self.nodes>200000:raise ValueError('Brave data resource limit')
        if isinstance(value,Reference):
            if value.name not in arguments:raise ValueError('Unbound Brave data variable')
            return arguments[value.name]
        if isinstance(value,dict):
            for key,item in value.items():value[key]=self.bind(item,arguments,depth+1)
        elif isinstance(value,list):
            for i,item in enumerate(value):value[i]=self.bind(item,arguments,depth+1)
        return value


def extract_data(text):
    if len(text)>8*1024*1024 or len(text.encode('utf-8'))>8*1024*1024:
        raise ValueError('Brave page exceeds 8 MiB')
    document=html.fromstring(text)
    for index,script in enumerate(document.iter('script')):
        if index>=128:raise ValueError('Brave script count limit')
        content=script.text or ''
        marker=re.search(r'(?:^|[,\n{])\s*data\s*:\s*(\[)',content)
        if marker:
            reader=LiteralReader(content,marker.start(1))
            value=reader.value()
            reader.bind(value,{},0)
            return {'data':value}
    raise ValueError('Brave page has no supported data array')
