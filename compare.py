#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import hashlib
import re
import argparse

from enum import Enum


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class entry_attr(Enum):
    SAME = 0
    NEW = 1
    DEL = 2
    DIFF = 3


class entry:
    def __init__(self, parent, path, attr):
        self._parent = parent
        self._path = path
        self._attr = attr

    def path(self):
        if self._parent:
            return os.path.join(self._parent.path(), self._path)
        else:
            return self._path

    def rpath(self):
        if self._parent:
            return os.path.join(self._parent.rpath(), self._path)
        else:
            return ''

    def attr(self):
        return self._attr

    def is_same(self):
        return self._attr == entry_attr.SAME

    def same(self):
        self._attr = entry_attr.SAME

    def type(self):
        pass

    def _entries_to_str(self, is_last_list, append):
        pass

    def _to_str(self, is_last_list):
        level = len(is_last_list)
        line = ''
        if level == 0:
            line += self._path + '\n'
        else:
            if not self.is_same():
                line += self.type()
                if (self._attr == entry_attr.NEW):
                    line += 'new       '
                elif (self._attr == entry_attr.DEL):
                    line += 'deleted   '
                else:
                    line += 'different '
                for is_last in is_last_list[:-1]:
                    if is_last:
                        line += '   '
                    else:
                        line += '│  '
                if is_last_list[-1]:
                    line += '└── '
                else:
                    line += '├── '
                line += self._path
                line += '\n'

        def append(str):
            nonlocal line
            line += str
        self._entries_to_str(is_last_list, append)
        return line

    def __str__(self) -> str:
        if self._parent:
            return self.path()
        else:
            return self._to_str([])


class dir(entry):
    def __init__(self, parent, path, attr):
        super().__init__(parent, path, attr)
        self._entries = []

    def add_entry(self, filename, attr):
        if os.path.isdir(os.path.join(self.path(), filename)):
            self._entries.append(dir(self, filename, attr))
        else:
            self._entries.append(file(self, filename, attr))

    def entries(self):
        return self._entries

    def type(self):
        return 'dir  '

    def _entries_to_str(self, is_last_list, append):
        entries = [e for e in self._entries if not e.is_same()]
        entries.sort(key=lambda e: e._path)
        if entries:
            for e in entries[:-1]:
                append(e._to_str(is_last_list + [False]))
            append(entries[-1]._to_str(is_last_list + [True]))


class file(entry):
    def __init__(self, parent, path, attr):
        super().__init__(parent, path, attr)

    def type(self):
        return 'file '


class comparator:
    def __init__(self, src, dest, ignore):
        self._src = os.path.abspath(src)
        self._dest = os.path.abspath(dest)
        self._result = None
        self._ignore = [re.compile(e) for e in ignore]

    def compare_entry(self, ent):
        if isinstance(ent, dir) and ent.attr() == entry_attr.DIFF:
            src_files = set(os.listdir(os.path.join(self._src, ent.rpath())))
            dest_files = set(os.listdir(ent.path()))
            src_ignore = set()
            dest_ignore = set()
            for rgx in self._ignore:
                for f in src_files:
                    if rgx.match(f):
                        src_ignore.add(f)
                for f in dest_files:
                    if rgx.match(f):
                        dest_ignore.add(f)
            src_files.difference_update(src_ignore)
            dest_files.difference_update(dest_ignore)
            new_files = dest_files.difference(src_files)
            del_files = src_files.difference(dest_files)
            diff_files = src_files.intersection(dest_files)
            for f in new_files:
                ent.add_entry(f, entry_attr.NEW)
            for f in del_files:
                ent.add_entry(f, entry_attr.DEL)
            for f in diff_files:
                ent.add_entry(f, entry_attr.DIFF)
            for e in ent.entries():
                self.compare_entry(e)
            same = True
            for e in ent.entries():
                if not e.is_same():
                    same = False
                    break
            if same:
                ent.same()
        if isinstance(ent, dir) and ent.attr() == entry_attr.NEW:
            dest_files = set(os.listdir(ent.path()))
            dest_ignore = set()
            for rgx in self._ignore:
                for f in dest_files:
                    if rgx.match(f):
                        dest_ignore.add(f)
            dest_files.difference_update(dest_ignore)
            for f in dest_files:
                ent.add_entry(f, entry_attr.NEW)
            for e in ent.entries():
                self.compare_entry(e)
        if isinstance(ent, file) and ent.attr() == entry_attr.DIFF:
            if (md5(ent.path()) == md5(os.path.join(self._src, ent.rpath()))):
                ent.same()

    def compare(self):
        result = dir(None, self._dest, entry_attr.DIFF)
        self.compare_entry(result)
        self._result = result

    def print_result(self):
        if not self._result:
            self.compare()
        print(self._result, end='')


def main(src, dest, ignores):
    c = comparator(src, dest, ignores)
    c.compare()
    c.print_result()


def arg_dir(str):
    if not os.path.isdir(str):
        print('error: %s is not a directory' % (str))
        exit(1)
    return str


def arg_rgx(str):
    try:
        return re.compile(str)
    except:
        print('error: "%s" is not a valid regular expression' % (str))
        exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='compare 2 directories file by file.')
    parser.add_argument('src', metavar='SRC', type=arg_dir,
                        help='the directory which you want to compare the dest to')
    parser.add_argument('dest', metavar='DEST', type=arg_dir,
                        help='the directory that is being compared')
    parser.add_argument('-i', '--ignores', metavar='IGNORE', type=arg_rgx,
                        nargs='*', dest='ignores', help='regex expression of ignored patern')
    args = parser.parse_args()
    main(args.src, args.dest, args.ignores)
