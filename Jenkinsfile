library 'pipe@main' //，<>@<branchName>。Jenkins，plibJenkins，masterbranchtag。

def preCommands = "source ~/.bash_profile && cd /data/ratex/dc && rm -rf BatchSvr"
def postCommands = "source ~/.bash_profile && cd /data/ratex/scripts && ./stop BatchSvr && ./start BatchSvr"

def jobInfo = [
    nodeName: "master",
    projName: "gateway",
    name: "BatchSvr",
    appName: "BatchSvr",
    buildInfo: [ //
        pom: "pom.xml", //pom
        mode: "package",
        options: "-DskipTests" // mvn
    ],
 packageInfo: [ //
        name: "BatchSvr", //
        packageName: "BatchSvr" //，tar.gz。。
    ],
    deployInfo: [ //
        DEV:[ //'master'DEV，
            [
                hostName: "dev-java-server01",
                packageFile: "*-DEV-*.tar.gz",
                targetFolder: "/data/ratex/dc",
                preCommands: preCommands, //
                postCommands: postCommands //
            ]
        ],
        DEV12:[
            [
                hostName: "dev-java-server02",
                packageFile: "*-DEV12*.tar.gz",
                targetFolder: "/data/ratex/dc",
                preCommands: preCommands, //
                postCommands: postCommands //
            ],
            [
                hostName: "dev-java-server03",
                packageFile: "*-DEV12*.tar.gz",
                targetFolder: "/data/ratex/dc",
                preCommands: preCommands, //
                postCommands: postCommands //
            ]
        ],
        QA:[ //'testnet'DEV，
            [
                hostName: "testnet-java-main",
                packageFile: "*-QA-testnet*.tar.gz",
                targetFolder: "/data/ratex/dc",
                preCommands: preCommands, //
                postCommands: postCommands //
            ]
        ],	
        UAT:[ //'uat'，
            [
                hostName: "uat-java-main",
                packageFile: "*-UAT-uat*.tar.gz",
                targetFolder: "/data/ratex/dc",
                preCommands: preCommands, //
                postCommands: postCommands //
            ]
        ],
        PROD:[ //'release'，
            [
                hostName: "prod-java-main",
                packageFile: "*-PROD-release*.tar.gz",
                targetFolder: "/data/ratex/dc",
                preCommands: preCommands, //
                postCommands: postCommands //
            ],
			[
                hostName: "prod-java-standby",
                packageFile: "*-PROD-release*.tar.gz",
                targetFolder: "/data/ratex/dc",
                preCommands: preCommands, //
                postCommands: postCommands //
            ]
        ]
    ],
     releaseInfo: [ 
	 serverName: "artifactory", //，JenkinsArtifactory
      packages: [
		[ //
        fileName: "*-PRD*.tar.gz", //，
        serverName: "artifactory", //，JenkinsArtifactory
        repoName: "generic-release", //
        projectName: "erp", //
        packageName: "custodianerp", //
        props: "proj=erp" //，Artifactory
    ] 
      ]
    ]
]

building.buildJavaService(jobInfo) //Pipeline
