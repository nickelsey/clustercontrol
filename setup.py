from setuptools import setup


def get_long_description():
    """
    returns README.md
    """
    with open("README.md", encoding="utf8") as f:
        return f.read()


setup(
    name="clustercontrol",
    version=0.1,
    python_requires=">=3.7",
    url="https://github.com/nickelsey/clustercontrol",
    license="MIT",
    description="docker-based MPI cluster control helper",
    long_description=get_long_description(),
    log_description_content_type="text/markdown",
    author="Nick Elsey",
    author_email="nicholas.elsey@gmail.com",
    scripts=["bin/clustercontrol"],
)
