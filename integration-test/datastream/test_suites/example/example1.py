import os
import subprocess

# this file will consists of one or more pairs of test_xxx and verify_xxx functions that correspond to each test.
# all test functions will start with the prefix of 'test_' and verify functions with the prefix of 'verify_'
# verify function can be rolled into test function itself.

# verification function
def validate_basic():
    # check file has 100 events
    print "Verify function \n"
    assert int(str(subprocess.check_call(["wc", "-l", "eventGeneratorTrace.txt"])).split()[0]) == 100


# test function - always start with the prefix of 'test'
def test_basic():
    print "\n --Invoking test_basic -- \n"
    # Parse command line

    # read classpath in from the build/<module>/<module>.classpath file
    #cp_path = cp_from_runpy_path(sys.argv[0])

    # run event generator to generate 100 events into a file
    os.system("/home/rchalasa/projects/datastream/datastream/integration-test/datastream/script/testdep.sh")
    print "\n --End of test_basic -- \n"
