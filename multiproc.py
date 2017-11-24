#!/usr/bin/python -B

from __future__ import print_function

import sys, os, time, signal
import multiprocessing
import tempfile
import functools

def run(func, argList, nproc=None, timeout=3600, raiseExceptions=True):
    """
    Executes the given function for each element of the given array of arguments.
    A separate process is launched for each invocation. Each element in argList is
    a tuple consisting of zero or more elements that are to be expanded and passed
    as arguments to the given 'func'. Results are returned as an array of the same
    size as the input, each output element corresponding to the input element at
    the same index. To get clean log output in a multiprocessing context, console
    output is buffered such that stdout and stderr are redirected to a temporary
    file until the function has completed, and then written to stdout all at once.

    By default, any exceptions raised by the child processes are propagated to the
    caller. Unfortunately, this is sometimes causing all processes to freeze. As a
    workaround, exceptions can be disabled by setting raiseExceptions to False, in
    which case the exception and its stack trace are just printed to the console.
    """
    try:
        # Ctrl+C handling is very delicate in Python multiprocessing. The main
        # process must be made to ignore Ctrl+C before a child process Pool is
        # created; the original Ctrl+C handler must be restored after creating
        # the Pool; map_async() must be used instead of the blocking map(); and
        # there must be a timeout when waiting on the results, because signals
        # are otherwise ignored.
        origHandler = signal.signal(signal.SIGINT, signal.SIG_IGN)
        pool = multiprocessing.Pool(nproc)
        signal.signal(signal.SIGINT, origHandler)
        runner = functools.partial(_run, func, raiseEnabled=raiseExceptions)
        mapResult = pool.map_async(runner, argList)
        results = mapResult.get(timeout)  # wait for N seconds before terminating
        pool.close()
        return results
    except BaseException as e:
        pool.terminate()
        raise
    finally:
        pool.join()

def _runBuffered(func, raiseEnabled):
    """
    Executes the given function and returns the result. Buffers all console output
    (stdout & stderr) into a temporary file until the function has completed, and
    then writes it all to stdout at once. This makes console output readable when
    multiple processes are writing to stdout/stderr at the same time.
    """
    with tempfile.NamedTemporaryFile(mode="w+t", delete=True) as tmpfile:
        try:
            result = None
            stdout = sys.stdout
            stderr = sys.stderr
            sys.stdout = tmpfile
            sys.stderr = tmpfile
            result = func()
            return result
        except BaseException as e:
            # The main process sometimes freezes if an exception is raised by a
            # child process; this may be a bug in the multiprocessing module or
            # we may be using it wrong. Either way, as a dirty workaround we're
            # adding a short delay before raising exceptions across the process
            # boundary. This does not fix the problem, but makes it happen much
            # more rarely.
            if raiseEnabled:
                time.sleep(0.2)
                raise
            else:
                print(e)
        finally:
            sys.stdout = stdout
            sys.stderr = stderr
            tmpfile.flush()
            tmpfile.seek(0)
            log = tmpfile.read()
            print(log, end='')
            sys.stdout.flush()

def _run(func, args, raiseEnabled):
   if type(args) is tuple:
       return _runBuffered(lambda: func(*args), raiseEnabled)
   else:
       return _runBuffered(lambda: func(args), raiseEnabled)

######################################################################################
#
#  U N I T   T E S T S
#
######################################################################################

if __name__ == "__main__":

    import unittest
    import time, random, re
    import numpy as np

    class _TestMultiproc(unittest.TestCase):

        def test_run(self):
            args = [9, 3, 8, 1, 33]
            expected = [18, 6, 16, 2, 66]
            results = run(_testfunc, args)
            self.assertEqual(results, expected)

        def test_run_multiarg(self):
            args = [(1,2), (3,4)]
            expected = [5, 25]
            results = run(_testmultiarg, args)
            self.assertEqual(results, expected)

        def test_run_with_print(self):
            args = [1, 2, 3, 4, 5]
            expected = [2, 4, 6, 8, 10]
            results = run(_testprint, args)
            self.assertEqual(results, expected)

        def test_partial(self):
            args = [1, 2, 3, 4, 5]
            expected = [101, 104, 109, 116, 125]
            partialfunc = functools.partial(_testmultiarg, v2=10)
            results = run(partialfunc, args)
            self.assertEqual(results, expected)

        def test_exceptions(self):
            args = [1, 2, 3, 4, 5]
            self.assertRaises(ValueError, lambda: run(_testexc, args, raiseExceptions=True))

    def _testprint(idx):  # must be in global scope
        print("This is a print statement in child process #%d."%(idx))
        return idx * 2

    def _testfunc(v):  # must be in global scope
        time.sleep(random.random())
        return v * 2

    def _testmultiarg(v1, v2):
        result = v1 * v1 + v2 * v2
        return result

    def _testexc(idx):
        print("This is child process #%d raising a ValueError."%(idx))
        raise ValueError("This is an intentional exception from child process #%d."%(idx))

    print("--" * 35)
    suite = unittest.TestLoader().loadTestsFromTestCase(_TestMultiproc)
    unittest.TextTestRunner(verbosity=0).run(suite)
