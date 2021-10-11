# -*- coding: utf-8 -*-

import os
import subprocess

import dotenv


def get_git_commit_version():
    try:
        version = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip().decode('utf-8')
        try:
            with open('.code_version', 'w') as f:
                f.write(version)
                f.write('\n')
        except Exception as e:
            print(f'Failed to write to ./.code_version despite git rev-parse HEAD returning "{version}"')
            raise e
    except FileNotFoundError:
        try:
            with open('.code_version', 'r', encoding='utf8') as f:
                version = f.readline().strip()
                print(f'read version {version}')
        except:
            version = 'unknown'
    except subprocess.CalledProcessError:
        # not a git repo, so read the .code_verison file
        try:
            with open('.code_version', 'r', encoding='utf8') as f:
                version = f.readline().strip()
                print(f'read version {version}')
        except:
            version = 'unknown'
    return str(version)


__version__ = '0.1.3+' + get_git_commit_version()

env_file = os.getenv("ENV", ".local.env")
if env_file == '/root/.bashrc':
    # google colab environment, default to .env
    env_file = 'google.env'
dotenv.load_dotenv(env_file)
