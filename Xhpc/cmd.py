# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import re
import sys
from os.path import abspath, exists, isfile


def get_abspath(term_: str) -> str:
    """Get the absolute path of an existing path but keep the trailing
    slash if it was present in the relative / user-defined path (can be
    needed for command such as rsync).

    Parameters
    ----------
    term_ : str
        A path

    Returns
    -------
    term : str
        The abspath of the path
    """
    term = abspath(term_)
    if term_.endswith('/'):
        term += '/'
    return term


def check_term(args: dict, term_: str) -> str:
    """If the term is a character string starting with an alphanumeric
    character, then check here whether it is an existing file/folder not yet
    in absolute path format, or an executable. If it is an executable (i.e.,
    could well be a local executable not present in a PATH), it is preserved
    unchanged in the parsed command line, otherwise, it is replaced to it
    abspath.
    This is somewhat redundant with `get_term` but would speed up by avoiding
    looking up in the set of executables the term using complicated  relative
    paths (i.e.,  that do not start with an alphanumeric character).

    Parameters
    ----------
    args : dict
        All arguments
    term_ : str
        A space-separated term in a decomposed command line

    Returns
    -------
    term : str
        The space-separated term changed (or not) to abspath
    """
    if term_ in args['executables']:
        term = term_
    elif exists(term_):
        term = get_abspath(term_)
    else:
        term = term_
    return term


def get_term(args: dict, tdx: int, term_: str) -> str:
    """Get the abspath of the input command line term if it is a path
    and not an executable. The case of an empty term may arise when the
    command line contained quotes that were split.

    Parameters
    ----------
    args : dict
        All arguments
    tdx : int
        Index of the term in the command line
    term_ : str
        A space-separated term in a decomposed command line

    Returns
    -------
    term : str
        The space-separated term changed (or not) to abspath
    """
    term = ''
    if term_:
        if tdx == 0 or re.match('[a-zA-Z0-9]', term_):
            term = check_term(args, term_)
        elif exists(term_):
            term = get_abspath(term_)
        else:
            term = term_
    return term


def split_quoted(line: str, quote: str) -> list:
    """Split a line based on the quotes and labels the ports that were
    between quotes using a two-items tuple, with the first item being the
    split part and the second item being the quote.

    Parameters
    ----------
    line : str
        Command line that contains some quotes
    quote : str
        A simple or double quote symbol

    Returns
    -------
    line_split : list

    """
    ls = line.split(quote)
    line_split = [(y, quote) if x % 2 else (y, '') for x, y in enumerate(ls)]
    return line_split


def quote_split_line(line_: str) -> list:
    """Split a command line that may or may not contain quotes. If quotes
    are present, it is important to split on quotes too in order to identify
    potential quoted paths that would need to be changes to abspath.

    Parameters
    ----------
    line_ : str
        Command line to split

    Returns
    -------
    split_line : list
        Split command line where items are tuples such as (term, quoting)
    """
    if '"' in line_:
        line_split = split_quoted(line_, '"')
    elif "'" in line_:
        line_split = split_quoted(line_, "'")
    else:
        line_split = [(line_, '')]
    return line_split


def get_final_command_part(quote_part: list, quote: str) -> str:
    """Reconstitute the part of the command line that was split
    based on quoting (or not). If a split was indeed operated based
    on quoting, add the quote used to split back, around the
    expression.

    Parameters
    ----------
    quote_part : list
        Element of a command line that were split based on quoting
    quote : str
        Either " or ' quote character

    Returns
    -------
    final_part : str
        Reconstituted command line
    """
    final_part = ' '.join(quote_part)
    if quote:
        final_part = quote + final_part + quote
    return final_part


def get_separated_terms(part: str, separator: str) -> list:
    """Get the list of terms that may hide separated by a comma in the command.

    Parameters
    ----------
    part : str
        A space-separated term in a decomposed command line
    separator : str
        A comma or a colon or a semicolon

    Returns
    -------
    terms : list
        Either the term alone or the terms separated by a comma
    """
    terms = [part]
    if separator in part:
        terms = part.split(separator)
    return terms


def parse_line(line_: str, args: dict, paths: set, commands: list) -> None:
    """Turn the words that have a path into an abspath (except for
    executables), and prepend these with the environment variable pointing
    to the scratch folder is the option to move files to scratch is set.

    Parameters
    ----------
    line_ : str
        Current line of the script file.
    args : dict
        All arguments. Here only the following keys are of interest:
            input_fp : str
                Input script path (or double-quoted command)
            executables : set
                Conda and global paths executables
            abs : bool
                Change the existing paths in command line to absolute paths
    paths : set
        Paths to file or folders to move when using scratch folder
    commands : list
        Line of the script with paths potentially changed to abspaths
    """
    if not line_.strip() or line_[0] == '#':
        commands.append(line_.strip('\n'))
    else:
        parts = list()
        for (quote_part_, quote) in quote_split_line(line_.strip('\n')):
            quote_part = list()
            quote_part_split = quote_part_.split(' ')
            for tdx, part in enumerate(quote_part_split):
                comma_terms = get_separated_terms(part, ',')
                comma_separated = []
                for comma_term in comma_terms:
                    colon_terms = get_separated_terms(comma_term, ':')
                    colon_separated = []
                    for term_ in colon_terms:
                        # Get the term (an abspath if it is an existing path)
                        if term_.startswith('${SCRATCH_FOLDER}'):
                            term = term_
                        elif args['abspath']:
                            term = get_term(args, tdx, term_)
                        else:
                            term = term_
                        # If them found as being an absolute path
                        if term.startswith('/'):
                            # Add term to set of paths
                            paths.add(term)
                            # Use it in command from scratch area if requested
                            if args['move'] and term not in args['exclude']:
                                term = '${SCRATCH_FOLDER}%s' % term
                        colon_separated.append(term)
                    comma_separated.append(':'.join(colon_separated))
                quote_part.append(','.join(comma_separated))
            parts.append(get_final_command_part(quote_part, quote))
        commands.append(''.join(parts))


def parse_script(args: dict) -> None:
    """Parse the .sh file and collect the actual script commands.
    This results in extending the `args` dictionary with the "commands" and
    "paths" keys, pointing to the list of commands and sets of files/folders
    to move to/from scratch locations.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            input_fp : str
                Input script path (or double-quoted command)
            abspath : bool
                Parse command and change existing paths to abspath
    """
    paths = set()
    commands = list()
    with open(args['input_fp']) as f:
        # for each command of the script
        for line_ in f:
            if args['abspath'] or args['move']:
                # collect abspath for existing files/folders or keep words as is
                parse_line(line_.strip(), args, paths, commands)
            else:
                commands.append(line_)
    args['commands'] = commands
    args['paths'] = paths


def get_commands(args: dict) -> None:
    """Check whether the command is passed as a script file or directly as
    an inline command, and parse the commands to collect in the `args` object
    the commands and the paths of files/folders to potentially move to scratch.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following keys are of interest:
            input_fp : str
                Input script path (or double-quoted command)
    """
    if isfile(args['input_fp']):
        # if the script file exists
        parse_script(args)  # get commands and ins/outs from file content
    else:
        # if the script file does not exist: could be a direct command line
        print('Command line parsing not yet available')
        sys.exit(1)
