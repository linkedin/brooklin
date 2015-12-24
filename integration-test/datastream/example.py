import os

test = {
  "deployment_code": os.path.join(os.path.dirname(os.path.abspath(__file__)), "deployables/example_deployment.py"),
  "test_code": [
#      can run multiple test suites that share same deployment and configs
      os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_suites/example/example1.py")
#      os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_suites/example/example2.py")
   ],
  "perf_code": os.path.join(os.path.dirname(os.path.abspath(__file__)), "perf.py"),
  "configs_directory": os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs/")
}
