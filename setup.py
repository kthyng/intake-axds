from setuptools import setup


setup(
    use_scm_version={
        "write_to": "intake_axds/_version.py",
        "write_to_template": '__version__ = "{version}"',
        "tag_regex": r"^(?P<prefix>v)?(?P<version>[^\+]+)(?P<suffix>.*)?$",
    },
    entry_points={
        "intake.drivers": [
            "axds_cat = intake_axds.axds_cat:AXDSCatalog",
        ]
    },
)
