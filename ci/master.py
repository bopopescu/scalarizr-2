from buildbot.schedulers.basic import AnyBranchScheduler
from buildbot.schedulers.triggerable import Triggerable
from buildbot.changes.filter import ChangeFilter
from buildbot.process.factory import BuildFactory
from buildbot.steps.main import MainShellCommand
from buildscripts import steps as buildsteps


project = __opts__['project']


c['schedulers'].append(AnyBranchScheduler(
        name=project,
        change_filter=ChangeFilter(project=project, category='default'),
        builderNames=['{0} source'.format(project)]
))


c["schedulers"].append(Triggerable(
        name="{0} packaging".format(project),
        builderNames=["deb_packaging", "rpm_packaging"]
))


def push_to_github(__opts__):
    cwd = 'sandboxes/{0}/svn2git'.format(project)
    return [
            MainShellCommand(
                    command="""
                    cd sandboxes/{0}/svn2git
                    svn2git --rebase --verbose
                    git push origin main""".format(project),
                    description='Pushing commit to GitHub',
                    descriptionDone='Push commit to GitHub (trunk)'),
    ]


c['builders'].append(dict(
        name='{0} source'.format(project),
        subordinatenames=['ubuntu1004'],
        factory=BuildFactory(steps=
                buildsteps.svn(__opts__) +
                buildsteps.bump_version(__opts__, setter='cat > src/scalarizr/version') +
                buildsteps.source_dist(__opts__) +
                buildsteps.trigger_packaging(__opts__) +
                buildsteps.to_repo(__opts__, types=["deb", "rpm"]) +
                push_to_github(__opts__)
        )
))
