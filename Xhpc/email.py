# ----------------------------------------------------------------------------
# Copyright (c) 2022, Franck Lejzerowicz.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import os

from os.path import dirname, isdir, isfile
from Xhpc.io_utils import get_first_line


def get_email_address(args: dict, config_fp: str) -> None:
    """
    Collect the email address from the edited config file.

    Parameters
    ----------
    args : dict
        All arguments. Here only the following key is of interest:
        config_email: bool
            Show the current email and potentially edit it
        Path to the file that may (or not) contains an email address
    config_fp : str
        Path to the file that may (or not) contains an email address
    """
    if isfile(config_fp):
        email_address = edit_config(config_fp, args['config_email'])
    else:
        email_address = create_config(config_fp)
    args['email_address'] = email_address


def validate_email(email: str) -> bool:
    """A not-so-fancy that a string may be a valid email address.

    Parameters
    ----------
    email : str
        A string that maz look like an email address

    Returns
    -------
    bool
        True if the string looks like an email, else False
    """
    if email.count('@') != 1:
        return False
    user, domain = email.split('@')
    if not len(user):
        return False
    if '.' not in domain:
        return False
    domain_split = domain.split('.')
    if 0 in map(len, domain_split):
        return False
    return True


def get_email() -> str:
    """Collect the email address interactively from the user.

    Returns
    -------
    email : str
        email address of the user.
    """
    email = input('Please enter a valid email address: ')
    if not validate_email(email):
        raise ValueError('%s not a valid email address' % email)
    return email


def edit_config(config_fp: str, config_email: bool):
    """
    Checks that the content of the existing config file
    is a valid email address.

    Parameters
    ----------
    config_fp : str
        Path to the file that may (or not) contains an email address
    config_email: bool
        Show the current email and potentially edit it

    Returns
    -------
    email : str
        email address of the user.
    """
    # parse the first line of the config file
    email = get_first_line(config_fp)
    if config_email:
        print("Registered email:", email)
        email = create_config(config_fp)
    return email


def create_config(config_fp: str) -> str:
    """Collect the email address interactively from the user
    and write it somewhere it can be reused.

    Parameters
    ----------
    config_fp : str
        Path to the config file

    Returns
    -------
    email : str
        email address of the user.
    """
    email = get_email()
    write_email(config_fp, email)
    return email


def write_email(config_fp: str, email: str):
    """Write the email address somewhere it can be reused.

    Parameters
    ----------
    config_fp : str
        Path to the config file
    email : str
        email address of the user.
    """
    if not isdir(dirname(config_fp)):
        os.makedirs(dirname(config_fp))
    with open(config_fp, 'w') as o:
        o.write('%s\n' % email)
        o.close()
    print('Written: %s' % config_fp)
