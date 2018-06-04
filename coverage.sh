
python3-coverage run --source chore setup.py test &> /dev/null
python2.6-coverage run -a --source chore setup.py test &> /dev/null
python2.7-coverage run -a --source chore setup.py test &> /dev/null
python3-coverage report -m

