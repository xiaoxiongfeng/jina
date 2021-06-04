"""Module for wrapping Jina Hub API calls."""

import argparse
import json
from typing import Dict

from .. import __version__ as jina_version
from ..excepts import (
    HubLoginRequired,
    ImageAlreadyExists,
)
from ..helper import (
    get_readable_size,
)
from ..importer import ImportExtensions
from ..logging.logger import JinaLogger
from ..logging.profile import TimeContext


class HubIO:
    """:class:`HubIO` provides the way to interact with Jina Hub registry.

    You can use it with CLI to package a directory into a Jina Hub image and publish it to the world.

    Examples:
        - :command:`jina hub build my_pod/` build the image
        - :command:`jina hub build my_pod/ --push` build the image and push to the public registry
        - :command:`jina hub pull jinahub/pod.dummy_mwu_encoder:0.0.6` to download the image
    """

    def __init__(self, args: 'argparse.Namespace'):
        """Create a new HubIO.

        :param args: arguments
        """
        self.logger = JinaLogger(self.__class__.__name__, **vars(args))
        self.args = args

    def new(self, no_input: bool = False) -> None:
        """
        Create a new executor using cookiecutter template.

        :param no_input: Argument to avoid prompting dialogue (just to be used for testing)
        """
        with ImportExtensions(required=True):
            from cookiecutter.main import cookiecutter
            import click  # part of cookiecutter

        cookiecutter_template = self.args.template
        if self.args.type == 'app':
            cookiecutter_template = 'https://github.com/jina-ai/cookiecutter-jina.git'
        elif self.args.type == 'pod':
            cookiecutter_template = (
                'https://github.com/jina-ai/cookiecutter-jina-hub.git'
            )

        try:
            cookiecutter(
                template=cookiecutter_template,
                overwrite_if_exists=self.args.overwrite,
                output_dir=self.args.output_dir,
                no_input=no_input,
            )
        except click.exceptions.Abort:
            self.logger.info('nothing is created, bye!')

    def push(
        self,
        name: Optional[str] = None,
        build_result: Optional[Dict] = None,
    ) -> None:
        """Push image to Jina Hub.

        :param name: name of image
        :param build_result: dictionary containing the build summary
        :return: None
        """
        name = name or self.args.name
        try:
            # check if image exists
            # fail if it does
            if (
                self.args.no_overwrite
                and build_result
                and self._image_version_exists(
                    build_result['manifest_info']['name'],
                    build_result['manifest_info']['version'],
                    jina_version,
                )
            ):
                raise ImageAlreadyExists(
                    f'Image with name {name} already exists. Will NOT overwrite.'
                )
            else:
                self.logger.debug(
                    f'Image with name {name} does not exist. Pushing now...'
                )

            self._push_docker_hub(name)

            if not build_result:
                file_path = get_summary_path(name)
                if os.path.isfile(file_path):
                    with open(file_path) as f:
                        build_result = json.load(f)
                else:
                    self.logger.error(
                        f'can not find the build summary file.'
                        f'please use "jina hub build" to build the image first '
                        f'before pushing.'
                    )
                    return

            if build_result:

                if build_result.get('details', None) and build_result.get(
                    'build_history', None
                ):
                    self._write_slack_message(
                        build_result,
                        build_result['details'],
                        build_result['build_history'],
                    )

        except Exception as e:
            self.logger.error(f'Error when trying to push image {name}: {e!r}')
            if isinstance(e, (ImageAlreadyExists, HubLoginRequired)):
                raise e

    def pull(self) -> None:
        """Pull docker image."""
        try:
            self._docker_login()
            with TimeContext(f'pulling {self.args.name}', self.logger):
                image = self._client.images.pull(self.args.name)
            if isinstance(image, list):
                image = image[0]
            image_tag = image.tags[0] if image.tags else ''
            self.logger.success(
                f'🎉 pulled {image_tag} ({image.short_id}) uncompressed size: {get_readable_size(image.attrs["Size"])}'
            )
        except Exception as ex:
            self.logger.error(
                f'can not pull image {self.args.name} from {self.args.registry} due to {ex!r}'
            )

    # alias of "new" in cli
    create = new
    init = new
