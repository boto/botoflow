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

"""
Various helper utils for the core
"""

def split_stack(stack):
    """
    Splits the stack into two, before and after the framework
    """
    stack_before, stack_after = list(), list()
    in_before = True
    for frame in stack:
        if 'flow/core' in frame[0]:
            in_before = False
        else:
            if in_before:
                stack_before.append(frame)
            else:
                stack_after.append(frame)
    return stack_before, stack_after


def filter_framework_frames(stack):
    """
    Returns a stack clean of framework frames
    """
    result = list()
    for frame in stack:
        # XXX Windows?
        if 'flow/core' not in frame[0]:
            result.append(frame)
    return result


def get_context_with_traceback(context):
    """
    Returns the very first context that contains traceback information
    """
    if context.tb_list:
        return context
    if context.parent:
        return get_context_with_traceback(context.parent)


def extract_stacks_from_contexts(context, stacks_list=None):
    """
    Returns a list of stacks extracted from the AsyncTaskContext in the right
    order
    """
    if stacks_list is None:
        stacks_list = list()

    if context.stack_list:
        stacks_list.append(context.stack_list)

    if context.parent is not None:
        return extract_stacks_from_contexts(context.parent, stacks_list)
    else:
        stacks_list.reverse()
        return stacks_list


def log_task_context(context, logger):
    """
    A helper for printing contexts as a tree
    """
    from .async_root_task_context import AsyncRootTaskContext

    root_context = None
    while root_context is None:
        if isinstance(context, AsyncRootTaskContext):
            root_context = context
        context = context.parent
    _log_task_context(root_context, logger)


def _log_task_context(context, logger, indent=0):
    logger.debug(" " * indent + '%r', context)
    indent += 2
    if not hasattr(context, 'children'):
        return
    for sub_context in context.children.union(context.daemon_children):
        _log_task_context(sub_context, logger, indent)
