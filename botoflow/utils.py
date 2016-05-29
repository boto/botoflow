# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""**INTERNAL**
"""
import re
import hashlib
import time
import random

from collections import namedtuple

import six

from .workflow_definition import WorkflowDefinition

WorkflowDetails = namedtuple('WorkflowDetails',
                             'name version skip_registration '
                             'registration_options')



def str_or_NONE(value):
    """If the `value` is `None` returns a "NONE"",
    otherwise returns `str(value)`
    """
    if value is None:
        return 'NONE'
    return str(value)


def random_sha1_hash():
    hash = hashlib.sha1()
    hash.update(six.b(str(time.time())))
    hash.update(six.b(str(random.random())))
    return hash.hexdigest()


def extract_workflow_details_from_class(cls):
    workflow_options = cls.swf_options['workflow']
    if workflow_options['version'] is None:
        raise AttributeError("workflow version must be set")

    name = workflow_options['name']
    version = workflow_options['version']
    skip_registration = workflow_options['skip_registration']

    # generate a default workflow name if one is missing
    if workflow_options['name'] is None:
        name = cls.__name__

    return WorkflowDetails(name, version, skip_registration,
                           cls.swf_options['workflow_registration_options'])


def pairwise(iterable):
    """
    from the itertools recipes
    s -> (s0,s1), (s1,s2), (s2, s3), (s3, None)...
    """
    a = next(iterable)
    b = None
    while True:
        b = None
        try:
            b = next(iterable)
        except StopIteration:
            break
        yield a, b
        a = b
    yield a, b


def extract_workflows_dict(workflow_definitions):
    workflows = dict()
    for workflow_definition in workflow_definitions:
        if not issubclass(workflow_definition, WorkflowDefinition):
            raise TypeError("workflow parameter must be an subclass of the "
                            "WorkflowDefinition")

        # extract activities info from the class
        for workflow_type, func_name in \
            six.iteritems(workflow_definition._workflow_types):

            namever = (workflow_type.name, workflow_type.version)
            workflows[namever] = (workflow_definition, workflow_type,
                                  func_name)

    return workflows


# regex for translating kwarg case
_first_cap_replace = re.compile(r'(.)([A-Z][a-z]+)')
_remainder_cap_replace = re.compile(r'([a-z0-9])([A-Z])')


def camel_keys_to_snake_case(dictionary):
    """
    Translate a dictionary containing camelCase keys into dictionary with
    snake_case keys that match python kwargs well.
    """
    output = {}
    for original_key in dictionary.keys():
        # insert an underscore before any word beginning with a capital followed by lower case
        translated_key = _first_cap_replace.sub(r'\1_\2', original_key)
        # insert an underscore before any remaining capitals that follow lower case characters
        translated_key = _remainder_cap_replace.sub(r'\1_\2', translated_key).lower()
        output[translated_key] = dictionary[original_key]
    return output


def snake_keys_to_camel_case(dictionary):
    """
    Translate a dictionary containing snake_case keys into dictionary with
    camelCase keys as required for decision dicts.
    """
    output = {}
    for original_key in dictionary.keys():
        components = original_key.split('_')
        translated_key = components[0] + ''.join([component.title() for component in components[1:]])
        output[translated_key] = dictionary[original_key]
    return output
