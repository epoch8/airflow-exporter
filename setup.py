from os import path

from setuptools import setup

root_dir = path.abspath(path.dirname(__file__))

readme_file = path.join(root_dir, "README.md")
with open(readme_file) as f:
    readme = f.read()

setup(
    name="airflow-exporter",
    use_scm_version=True,
    description="Airflow plugin to export dag and task based metrics to Prometheus.",
    long_description=readme,
    long_description_content_type="text/markdown",
    classifiers=[
        "Environment :: Web Environment",
        "Intended Audience :: System Administrators",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Topic :: System :: Monitoring",
    ],
    keywords="airflow plugin prometheus exporter metrics",
    url="https://github.com/epoch8/airflow-exporter",
    packages=["airflow_exporter"],
    setup_requires=['setuptools_scm'],
    install_requires=[
        "apache-airflow>=2.0.0",
        "prometheus_client>=0.4.2",
    ],
    entry_points={
        "airflow.plugins": [
            "AirflowPrometheus = airflow_exporter.prometheus_exporter:AirflowPrometheusPlugins"
        ]
    },
)
