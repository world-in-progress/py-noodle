import os
import sys
sys.path.append(os.getcwd())    # ensure the current working directory is in sys.path

import click
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from .noodle import noodle

@click.command()
@click.argument('template_name', type=str)
def re_privatize(template_name: str):
    """
    Re-privatize resource nodes' launch params based on the specified template,
    if the resource node template's privatization function is modified.
    """
    noodle.re_privatize(template_name)

@click.group()
def cli():
    """Pynoodle Command Line Interface"""
    pass
cli.add_command(re_privatize)