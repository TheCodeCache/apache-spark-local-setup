Spark version: 3.3.0-SNAPSHOT

This is about how to set up Apache Spark open-source code in your local Eclipse IDE.

PFB steps:

1. checkout this repo on the local machine
2. open scala eclipse ide, with workspace set till this folder <local path>/apache-spark-local-setup
3. import the spark code using "Existing Maven Projects" from the "import" wizard
4. let it configure the codebase and "building workspace" progress bar to complete
5. once it is done, select all the projects and do a "maven update project"
6. select the parent project, right click and do run as "Maven build", 
 use skipTests=true (to avoid running test cases) as well as -Dmaven.skip.tests=true (to avoid compiling test cases) in order to build faster
7. select all the project and refresh it,
8. go to this path: 
	<windows local path>\apache-spark-local-setup\spark\sql\core\src\main\scala\org\apache\spark\sql\test\TestBatchSetup.scala
	<windows local path>\apache-spark-local-setup\spark\sql\core\src\main\scala\org\apache\spark\sql\test\TestMe.scala

The above steps have been tested on Windows Machine ( v8.1 and v10)

Requirements:

	scala-ide (latest): http://scala-ide.org/download/sdk.html
	java: basically java8 (tested on jdk1.8.0_301 or jdk1.8.0_121 or jdk1.8.0_201)
	maven: 3.6.3 or later version (I've tested specifically on 3.6.3)
	eclipse.ini:
		-startup
		plugins/org.eclipse.equinox.launcher_1.4.0.v20161219-1356.jar
		--launcher.library
		plugins/org.eclipse.equinox.launcher.win32.win32.x86_64_1.1.500.v20170531-1133
		-vmargs
		-XX:+UseG1GC
		-XX:+UseStringDeduplication
		-Xmx2560m
		-Xms512m
	(notice: I've used G1 as Garbage Collector and a max heap size of 2.5 GB though, it will work even on 2 GB

Observation:

	building spark code is more smooth with G1 than other Garbage Collectors (like CMS), 
	it consumes less heap as well, mostly aorund 1 GB heap, sometimes it do touch around 1.9 GB or 2.1 GB

Possible Challanges (while building):

	in case of StackOverflowError, use -xss 512kb in the above -vmargs as part of eclipse.ini file

Once the codebase is all set then take a walk of the codebase, start debugging the source code,
	make changes in the source code for deeper understanding the flow etc. now spark is all yours : D

I'm sure you'll enjoy like I did : p : D 

Happy Learning !!
