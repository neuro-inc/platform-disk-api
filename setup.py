from setuptools import find_packages, setup


install_requires = (
    "aiohttp==3.6.2",
    "yarl==1.5.1",
    "neuro_auth_client==19.11.26",
    "marshmallow==3.8.0",
    "aiohttp-apispec==2.2.1",
    "platform-logging==0.3",
    "aiohttp-cors==0.7.0",
)

setup(
    name="platform-disk-api",
    version="0.0.1b1",
    url="https://github.com/neuromation/platform-disk-api",
    packages=find_packages(),
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
