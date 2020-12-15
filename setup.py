from setuptools import find_packages, setup


setup_requires = ("setuptools_scm",)

install_requires = (
    "aiohttp==3.7.3",
    "yarl==1.6.3",
    "neuro_auth_client==19.11.26",
    "marshmallow==3.9.1",
    "aiohttp-apispec==2.2.1",
    "platform-logging==0.3",
    "aiohttp-cors==0.7.0",
)

setup(
    name="platform-disk-api",
    url="https://github.com/neuromation/platform-disk-api",
    use_scm_version={
        "git_describe_command": "git describe --dirty --tags --long --match v*.*.*",
    },
    packages=find_packages(),
    setup_requires=setup_requires,
    install_requires=install_requires,
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "platform-disk-api=platform_disk_api.api:main",
            "platform-disk-api-watcher=platform_disk_api.usage_watcher:main",
        ]
    },
    zip_safe=False,
)
