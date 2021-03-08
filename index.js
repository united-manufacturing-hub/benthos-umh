import fs from 'fs'
import path from 'path'

import mkdirp from 'mkdirp'
import jsYaml from 'js-yaml'
import jszip from 'jszip'

const { load: yamlLoad, dump: yamlDump } = jsYaml
const { loadAsync: zipLoadAsync } = jszip

const staticConfigName = 'config.yaml'

export const createArtifactsFrom = ({
  tmpRoot,
  configRoot,
  distRoot
}) => async ({ name, artifacts = [] }) => {
  if (!tmpRoot) throw new Error('Missing tmpRoot')
  if (!configRoot) throw new Error('Missing configRoot')
  if (!distRoot) throw new Error('Missing distRoot')

  const srcZipPath = path.resolve(tmpRoot, `${name}.zip`)
  const srcZipBuffer = await fs.promises.readFile(srcZipPath)
  const createArtifact = createArtifactFrom(srcZipBuffer, configRoot, distRoot)

  await mkdirp(path.resolve(distRoot))
  return Promise.all(artifacts.map(createArtifact))
}

export const loadJson = async (name) => {
  const data = await fs.promises.readFile(name)
  return JSON.parse(data)
}

const createArtifactFrom = (srcZipBuffer, configRoot, distRoot) => async (
  artifactName
) => {
  const configName = `${artifactName}.yaml`
  const configPath = path.resolve(configRoot, configName)
  const configData = await loadYamlToBuffer(configPath)

  const artifactBuffer = await generateZipFrom(srcZipBuffer, [
    { name: staticConfigName, data: configData }
  ])

  const outputName = `${artifactName}.zip`
  const outputPath = path.resolve(distRoot, outputName)
  await fs.promises.writeFile(outputPath, artifactBuffer)
  return { outputPath }
}

const loadYamlToBuffer = async (name) => {
  const buf = await fs.promises.readFile(path.resolve(name))
  const data = await yamlLoad(buf)
  return yamlDump(data)
}

const generateZipFrom = async (srcZipBuffer, files = []) => {
  const zip = await zipLoadAsync(srcZipBuffer)

  for (const { name, data } of files) {
    zip.file(name, data, { unixPermissions: '644' })
  }

  return zip.generateAsync({
    type: 'nodebuffer',
    compression: 'DEFLATE',
    platform: process.platform
  })
}
