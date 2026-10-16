import os
from pathlib import Path
import xml.etree.ElementTree as ET


def build_summary(results):
    reports = {path.read_bytes() for path in results.rglob('coverage.cobertura.xml')}
    trx_files = list(results.rglob('*.trx'))
    if len(reports) != 1 or len(trx_files) != 1:
        raise ValueError('Expected one distinct combined coverage report and one TRX file; clear old TestResults.')

    coverage = ET.fromstring(reports.pop())
    trx = ET.parse(trx_files[0])
    ns = {'t': 'http://microsoft.com/schemas/VisualStudio/TeamTest/2010'}
    counters = trx.find('t:ResultSummary/t:Counters', ns)
    total = int(counters.attrib['total'])
    passed = int(counters.attrib['passed'])
    if total == 0 or passed != total:
        raise ValueError('Only a non-empty, fully passing test run can update the summary.')

    layers = dict.fromkeys(('Unit', 'Integration', 'EndToEnd'), 0)
    for method in trx.findall('t:TestDefinitions/t:UnitTest/t:TestMethod', ns):
        class_name = method.attrib['className']
        for layer in layers:
            if f'.{layer}.' in class_name:
                layers[layer] += 1
                break
    if sum(layers.values()) != total:
        raise ValueError('Every test must belong to a documented test layer.')

    lines_covered = int(coverage.attrib['lines-covered'])
    lines_valid = int(coverage.attrib['lines-valid'])
    branches_covered = int(coverage.attrib['branches-covered'])
    branches_valid = int(coverage.attrib['branches-valid'])
    line_rate = 100 * lines_covered / lines_valid
    branch_rate = 100 * branches_covered / branches_valid
    run_url = os.environ.get('TEST_RUN_URL')
    commit = os.environ.get('TEST_COMMIT')
    repository_url = os.environ.get('TEST_REPOSITORY_URL')
    source = 'Measured from a local test run.'
    if run_url and commit and repository_url:
        source = f'Measured by [CI]({run_url}) on commit [`{commit[:7]}`]({repository_url}/commit/{commit}).'

    return f'''**{passed} tests passed** · **{line_rate:.2f}% line coverage** ({lines_covered}/{lines_valid}) · **{branch_rate:.2f}% branch coverage** ({branches_covered}/{branches_valid}).
Unit: **{layers['Unit']}** · Integration: **{layers['Integration']}** · E2E: **{layers['EndToEnd']}**.

Combined coverage of Scheduler, Client SDK and Worker SDK; generated `obj` files are excluded.
{source}'''


def main():
    root = Path(__file__).resolve().parents[1]
    summary = build_summary(root / 'tests/WorkflowScheduler.Tests/TestResults')
    readme = root / 'README.md'
    text = readme.read_text(encoding='utf-8')
    start_marker = '<!-- test-summary:start -->'
    end_marker = '<!-- test-summary:end -->'
    if text.count(start_marker) != 1 or text.count(end_marker) != 1:
        raise ValueError('README must contain exactly one test summary marker pair.')
    start = text.index(start_marker) + len(start_marker)
    end = text.index(end_marker)
    if end < start:
        raise ValueError('Test summary markers are out of order.')
    readme.write_text(text[:start] + '\n' + summary + '\n' + text[end:], encoding='utf-8')
    print(summary)
    if path := os.environ.get('GITHUB_STEP_SUMMARY'):
        with Path(path).open('a', encoding='utf-8') as stream:
            stream.write('## Test results\n\n' + summary + '\n')


if __name__ == '__main__':
    main()
