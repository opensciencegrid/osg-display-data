
from distutils.core import setup

setup(name="OSG_Display_Data",
      version="0.1",
      author="Brian Bockelman",
      author_email="bbockelm@cse.unl.edu",
      description="Scripts and tools to generate the OSG Display's data.",

      package_dir={"": "src"},
      packages=["osg_display"],

      data_files=[("/etc/cron.d", ["src/scripts/osg_display.cron"]),
          ("/etc/osg_display", ["config/graphs.conf", "src/script/osg_display.condor.cron"]),
          ("/usr/bin", ["src/scripts/osg_display"])],

     )

