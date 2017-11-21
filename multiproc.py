#!/usr/bin/python -B

from __future__ import print_function

import sys, os, time, signal
import multiprocessing
import tempfile

def run(func, argList, nproc=None, timeout=3600):
    """
    Executes the given function for each element of the given array of arguments.
    A separate process is launched for each invocation. Each element in argList is
    a tuple consisting of zero or more elements that are to be expanded and passed
    as arguments to the given 'func'. Results are returned as an array of the same
    size as the input, each output element corresponding to the input element at
    the same index. To get clean log output in a multiprocessing context, console
    output is buffered such that stdout and stderr are redirected to a temporary
    file until the function has completed, and then written to stdout all at once.
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
        funcList = [func] * len(argList)
        mapResult = pool.map_async(_run, zip(funcList, argList))
        results = mapResult.get(timeout)  # wait for N seconds before terminating
        pool.close()
        return results
    except Exception as e:
        pool.terminate()
        raise
    finally:
        pool.join()

def _runBuffered(func):
    """
    Executes the given function and returns the result. Buffers all console output
    (stdout & stderr) into a temporary file until the function has completed, and
    then writes it all to stdout at once. This makes console output readable when
    multiple processes are writing to stdout/stderr at the same time.
    """
    with tempfile.NamedTemporaryFile(mode="w+t", delete=True) as tmpfile:
        try:
            stdout = sys.stdout
            stderr = sys.stderr
            sys.stdout = tmpfile
            sys.stderr = tmpfile
            return func()
        except BaseException as e:
            # The main process sometimes freezes if an exception is raised by a
            # child process; this may be a bug in the multiprocessing module or
            # we may be using it wrong. Either way, as a dirty workaround we're
            # adding a short delay before raising exceptions across the process
            # boundary. This does not fix the problem, but makes it happen much
            # more rarely.
            time.sleep(0.2)  # NOTE: 0.1 seconds is not enough
            raise
        finally:
            sys.stdout = stdout
            sys.stderr = stderr
            tmpfile.flush()
            tmpfile.seek(0)
            log = tmpfile.read()
            print(log, end='')
            sys.stdout.flush()

def _run(args):
   func, args = args
   if type(args) is tuple:
       return _runBuffered(lambda: func(*args))
   else:
       return _runBuffered(lambda: func(args))

######################################################################################
#
#  U N I T   T E S T S
#
######################################################################################

if __name__ == "__main__":

    import unittest

    class _TestMultiproc(unittest.TestCase):

        def test_run(self):
            args = [9, 3, 8, 1, 33]
            expected = [18, 6, 16, 2, 66]
            results = run(_testfunc, args)
            self.assertEqual(results, expected)

        def test_run_with_print(self):
            args = [1, 2, 3, 4, 5]
            expected = [2, 4, 6, 8, 10]
            results = run(_testprint, args)
            self.assertEqual(results, expected)

        def test_exceptions(self):
            args = [1, 2, 3, 4, 5]
            self.assertRaises(ValueError, lambda: run(_testexc, args))

    def _testprint(idx):  # must be in global scope
        print("This is a print statement in child process #%d."%(idx))
        return idx * 2

    def _testfunc(v):  # must be in global scope
        import time, random  # randomize ordering
        time.sleep(random.random())
        return v * 2

    def _testexc(idx):
        print("This is child process #%d raising a ValueError."%(idx))
        raise ValueError("This is an intentional exception from child process #%d."%(idx))

    print("--" * 35)
    suite = unittest.TestLoader().loadTestsFromTestCase(_TestMultiproc)
    unittest.TextTestRunner(verbosity=0).run(suite)
