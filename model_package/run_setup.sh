
#!/bin/bash

rm -rf $PWD"/dist" $PWD"/model_scoring.egg-info"

python $PWD/setup.py sdist --dist-dir=$PWD/dist --formats=gztar

