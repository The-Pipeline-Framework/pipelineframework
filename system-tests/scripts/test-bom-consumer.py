#!/usr/bin/env python3
"""Compile a versionless consumer/processor-path fixture over an independently pinned local test BOM."""
import json
import subprocess
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ["conversation-context-connector", "decision-query-connector", "decision-query-jev-connector",
             "representation-provider-file", "host-quickbooks-mcp"]
repository = ROOT / ".m2/repository"
with tempfile.TemporaryDirectory(prefix="tpf-bom-consumer-") as temporary:
    directory = Path(temporary)
    config = json.loads((ROOT / "system-tests/components.yml").read_text())
    selected = {"components": {name: {"repository": value["repository"], "sha": "a" * 40,
                                      "mavenVersion": f"91.{index}.0"}
                               for index, (name, value) in enumerate(config["components"].items()) if value["kind"] == "maven"}}
    version = selected["components"]["connectors"]["mavenVersion"]
    resolved = directory / "resolved.json"
    resolved.write_text(json.dumps(selected))
    subprocess.run(["node", str(ROOT / "system-tests/scripts/materialize-tested-bom.mjs"),
                    "--source", str(ROOT / "framework/bom/pom.xml"), "--resolvedSet", str(resolved),
                    "--repository", str(repository), "--coordinationSha", "a" * 40,
                    "--outputResolvedSet", str(resolved)], check=True)
    bom_version = json.loads(resolved.read_text())["coordinationBom"]["mavenVersion"]
    for artifact in ARTIFACTS:
        target = repository / "org/pipelineframework" / artifact / version
        target.mkdir(parents=True, exist_ok=True)
        (target / f"{artifact}-{version}.pom").write_text(f'<project><modelVersion>4.0.0</modelVersion><groupId>org.pipelineframework</groupId><artifactId>{artifact}</artifactId><version>{version}</version></project>')
        with zipfile.ZipFile(target / f"{artifact}-{version}.jar", "w"):
            pass
    dependencies = "".join(f"<dependency><groupId>org.pipelineframework</groupId><artifactId>{artifact}</artifactId></dependency>" for artifact in ARTIFACTS)
    paths = "".join(f"<path><groupId>org.pipelineframework</groupId><artifactId>{artifact}</artifactId></path>" for artifact in ARTIFACTS)
    (directory / "pom.xml").write_text(f'''<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>test</groupId><artifactId>bom-consumer</artifactId><version>1.0</version>
<dependencyManagement><dependencies><dependency><groupId>org.pipelineframework</groupId><artifactId>pipelineframework-bom</artifactId><version>{bom_version}</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement>
<dependencies>{dependencies}</dependencies><build><plugins><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>3.14.1</version><configuration><release>21</release><annotationProcessorPaths>{paths}</annotationProcessorPaths></configuration></plugin></plugins></build></project>''')
    source = directory / "src/main/java/Consumer.java"
    source.parent.mkdir(parents=True)
    source.write_text("public class Consumer {}")
    subprocess.run([str(ROOT / "mvnw"), "-B", "-ntp", "-f", str(directory / "pom.xml"),
                    f"-Dmaven.repo.local={repository}", "compile"], cwd=ROOT, check=True)
    assert (directory / "target/classes/Consumer.class").exists()
    print(f"Versionless consumer and processor paths compiled using Connectors {version}; other component families have different versions.")
