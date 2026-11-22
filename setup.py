from setuptools import setup, find_packages

setup(
    name="dab_project",
    version="0.0.1",
    description="This contains the code in the ./src directory of the project",
    author="Xemita",
    author_email="jm.perales.pontes@gmail.com",
    packages=find_packages(where="./src"),
    package_dir={"": "./src"}, #indica la raíz del módulo
    install_requieres=["setuptools"],
    entry_points={
        "packages":[
            "main=dab_project.main:main"
        ]
    }

)