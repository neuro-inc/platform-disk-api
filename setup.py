from setuptools import find_packages, setup


install_requires = (
    "aiohttp==3.7.2",
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
    packages=find_packages(),
    use_scm_version={
        "tag_regex": r"(artifactory/)?(?P<version>.*)",
        "git_describe_command": (
            "git describe --dirty --tags --long --match artifactory/*.*.*"
        ),
    },
    setup_requires=["setuptools_scm"],
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
