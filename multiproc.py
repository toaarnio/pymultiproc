#!/usr/bin/python -B

import sys, os, signal
import multiprocessing
import tempfile

def run(func, argList, nproc=None, timeout=600):
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
    with tempfile.NamedTemporaryFile(delete=True) as tmpfile:
        try:
            stdout = sys.stdout
            stderr = sys.stderr
            sys.stdout = tmpfile
            sys.stderr = tmpfile
            return func()
        except Exception as e:
            import traceback  # embed stack trace into exception message
            raise type(e)(traceback.format_exc())
        finally:
            sys.stdout = stdout
            sys.stderr = stderr
            tmpfile.seek(0)
            log = tmpfile.read()
            print log,
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

    def _testfunc(v):  # must be in global scope
        import time, random  # randomize ordering
        time.sleep(random.random())
        return v * 2

    print "--" * 35
    suite = unittest.TestLoader().loadTestsFromTestCase(_TestMultiproc)
    unittest.TextTestRunner(verbosity=0).run(suite)
