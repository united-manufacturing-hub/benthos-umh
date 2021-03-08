import { createArtifactsFrom, loadJson } from './index.js'

const createArtifacts = createArtifactsFrom({
  tmpRoot: 'tmp',
  configRoot: 'config',
  distRoot: 'dist'
})

const build = async () => {
  const { benthos } = await loadJson('package.json')
  return createArtifacts(benthos)
}

const handleError = (err) => {
  console.error(err)
  process.exit(1)
}

const handleDone = (artifacts) => {
  for (const { outputPath } of artifacts) {
    console.log(`Built artifact: ${outputPath}`)
  }
}

build().then(handleDone).catch(handleError)
