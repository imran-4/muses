#!/bin/bash

export ANSIBLE_ERROR_ON_UNDEFINED_VARS=True
export ANSIBLE_HOST_KEY_CHECKING=False

echo 'Running: ansible-playbook -i hosts playbook.yml'
ansible-playbook -K playbook.yml # -e 'ansible_python_interpreter=/usr/bin/python3'


exit 0