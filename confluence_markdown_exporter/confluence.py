"""Confluence API documentation.

https://developer.atlassian.com/cloud/confluence/rest/v1/intro
"""

import functools
from math import e
import mimetypes
import os
import re
import sys
import copy
import base64
import requests
import hashlib
import json
from collections.abc import Set
from os import PathLike
from pathlib import Path
from string import Template
from typing import Literal
from typing import TypeAlias
from typing import cast
from typing import Optional
from urllib.parse import urlparse

import yaml
from atlassian import Confluence as ConfluenceApi
from atlassian import Jira
from atlassian.errors import ApiError
from bs4 import BeautifulSoup
from bs4 import NavigableString
from bs4 import Tag
from markdownify import ATX
from markdownify import MarkdownConverter
from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator
from pydantic import ValidationError
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from requests import HTTPError
from tqdm import tqdm

from confluence_markdown_exporter.utils.export import sanitize_filename
from confluence_markdown_exporter.utils.export import sanitize_key
from confluence_markdown_exporter.utils.export import save_file
from confluence_markdown_exporter.utils.table_converter import TableConverter

JsonResponse: TypeAlias = dict
StrPath: TypeAlias = str | PathLike[str]

DEBUG: bool = bool(os.getenv("DEBUG"))


class ApiSettings(BaseSettings):
    atlassian_username: str | None = Field(default=None)
    atlassian_api_token: str | None = Field(default=None)
    atlassian_pat: str | None = Field(default=None)
    atlassian_url: str = Field()

    @model_validator(mode="before")
    @classmethod
    def validate_auth(cls, data: dict) -> dict:
        if "atlassian_pat" in data:
            return data

        if "atlassian_username" in data and "atlassian_api_token" in data:
            return data

        msg = "Either ATLASSIAN_PAT or both ATLASSIAN_USERNAME and ATLASSIAN_API_TOKEN must be set."
        raise ValueError(msg)

    model_config = SettingsConfigDict(env_file=".env")


class ConverterSettings(BaseSettings):
    """Settings for the Markdown converter."""

    markdown_style: Literal["GFM", "Obsidian"] = Field(
        default="GFM",
        description="Markdown style to use for conversion. Options: GFM, Obsidian.",
    )
    page_path: str = Field(
        default="{space_name}/{homepage_title}/{ancestor_titles}/{page_title}.md",
        description=(
            "Path to store pages. Default: \n"
            "  {space_name}/{homepage_title}/{ancestor_titles}/{page_title}.md\n"
            "Variables:\n"
            "  {space_key}       - Space key\n"
            "  {space_name}      - Space name\n"
            "  {homepage_id}     - Homepage ID\n"
            "  {homepage_title}  - Homepage title\n"
            "  {ancestor_ids}    - Ancestor IDs (separated by '/')\n"
            "  {ancestor_titles} - Ancestor titles (separated by '/')\n"
            "  {page_id}         - Page ID\n"
            "  {page_title}      - Page title"
        ),
    )
    attachment_path: str = Field(
        default="{space_name}/attachments/{attachment_id}-{attachment_title}",
        description=(
            "Path to store attachments. Default: \n"
            "  {space_name}/attachments/{attachment_id}-{attachment_title}\n"
            "Variables:\n"
            "  {space_key}           - Space key\n"
            "  {space_name}          - Space name\n"
            "  {homepage_id}         - Homepage ID\n"
            "  {homepage_title}      - Homepage title\n"
            "  {ancestor_ids}        - Ancestor IDs (separated by '/')\n"
            "  {ancestor_titles}     - Ancestor titles (separated by '/')\n"
            "  {attachment_id}       - Attachment ID\n"
            "  {attachment_title}    - Attachment title\n"
            "  {attachment_file_id}  - Attachment file ID\n"
            "  {attachment_extension} - Attachment file extension (including leading dot)"
        ),
    )
    output_root_path: StrPath = Field(
        default=".",
        description="Root path for exported files, used for assets and other global exports",
    )


try:
    api_settings = ApiSettings()  # type: ignore reportCallIssue as the parameters are read via env file
except ValidationError:
    print(
        "Please set the required environment variables: "
        "ATLASSIAN_URL and either both ATLASSIAN_USERNAME and ATLASSIAN_API_TOKEN "
        "or ATLASSIAN_PAT\n\n"
        "Read the README.md for more information."
    )
    sys.exit(1)

converter_settings = ConverterSettings()

if api_settings.atlassian_pat:
    auth_args = {"token": api_settings.atlassian_pat}
else:
    auth_args = {
        "username": api_settings.atlassian_username,
        "password": api_settings.atlassian_api_token,
    }

confluence = ConfluenceApi(url=api_settings.atlassian_url, timeout=360, **auth_args)

# If JIRA_PAT_TOKEN env var is specified, override the auth args for jira client
jira_pat_token = os.getenv("JIRA_PAT_TOKEN")
if jira_pat_token:
    jira_auth_args = {"token": jira_pat_token}
else:
    jira_auth_args = auth_args

# Use JIRA_URL if specified, otherwise use the same Atlassian URL as Confluence
jira_url = os.getenv("JIRA_URL") or api_settings.atlassian_url

jira = Jira(url=jira_url, timeout=360, **jira_auth_args)


class JiraIssue(BaseModel):
    key: str
    summary: str
    description: str | None
    status: str

    @classmethod
    def from_json(cls, data: JsonResponse) -> "JiraIssue":
        fields = data.get("fields", {})
        return cls(
            key=data.get("key", ""),
            summary=fields.get("summary", ""),
            description=fields.get("description", ""),
            status=fields.get("status", {}).get("name", ""),
        )

    @classmethod
    @functools.lru_cache(maxsize=100)
    def from_key(cls, issue_key: str) -> "JiraIssue":
        issue_data = cast(JsonResponse, jira.get_issue(issue_key))
        return cls.from_json(issue_data)


class User(BaseModel):
    username: str
    display_name: str
    email: str

    @classmethod
    def from_json(cls, data: JsonResponse) -> "User":
        return cls(
            username=data.get("username", ""),
            display_name=data.get("displayName", ""),
            email=data.get("email", ""),
        )

    @classmethod
    @functools.lru_cache(maxsize=100)
    def from_username(cls, username: str) -> "User":
        return cls.from_json(cast(JsonResponse, confluence.get_user_details_by_username(username)))

    @classmethod
    @functools.lru_cache(maxsize=100)
    def from_userkey(cls, userkey: str) -> "User":
        return cls.from_json(cast(JsonResponse, confluence.get_user_details_by_userkey(userkey)))

    @classmethod
    @functools.lru_cache(maxsize=100)
    def from_accountid(cls, accountid: int) -> "User":
        return cls.from_json(
            cast(JsonResponse, confluence.get_user_details_by_accountid(accountid))
        )


class Organization(BaseModel):
    spaces: list["Space"]

    @property
    def pages(self) -> list[int]:
        return [page for space in self.spaces for page in space.pages]

    def export(self, export_path: StrPath) -> None:
        export_pages(self.pages, export_path)

    @classmethod
    def from_json(cls, data: JsonResponse) -> "Organization":
        return cls(
            spaces=[Space.from_json(space) for space in data.get("results", [])],
        )

    @classmethod
    @functools.lru_cache(maxsize=100)
    def from_api(cls) -> "Organization":
        return cls.from_json(
            cast(
                JsonResponse,
                confluence.get_all_spaces(
                    space_type="global", space_status="current", expand="homepage"
                ),
            )
        )


class Space(BaseModel):
    key: str
    name: str
    description: str
    homepage: Optional[int] = None

    @property
    def pages(self) -> list[int]:
        if self.homepage is None:
            return []
        
        # Implement paginated fetching of pages
        all_pages = []
        
        # Get the page generator
        page_generator = confluence.get_all_pages_from_space_as_generator(
            self.key, 
            limit=200,
            expand=None
        )
        
        # Now iterate over each page in the generator
        for page in page_generator:
            # Process individual page here
            page_id = int(page["id"])
            all_pages.append(page_id)
        
        # Return homepage and all descendants
        return all_pages

    def export(self, export_path: StrPath) -> None:
        # Define cache file path for storing page IDs
        output_path = Path(export_path)
        cache_dir = output_path / ".cache"
        os.makedirs(cache_dir, exist_ok=True)
        cache_file = cache_dir / "pages.txt"
        
        # Check if cache file exists
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r") as f:
                    all_pages = [line.strip() for line in f if line.strip()]
                print(f"Loaded {len(all_pages)} page IDs from cache file for space {self.key}")
            except Exception as e:
                print(f"Error loading cached page IDs: {e}")
                # Fallback to fetching pages
                all_pages = self.pages
                # Save fetched pages to cache
                with open(cache_file, "w") as f:
                    for page_id in all_pages:
                        f.write(f"{page_id}\n")
        else:
            # Fetch pages and save to cache
            all_pages = self.pages
            try:
                with open(cache_file, "w") as f:
                    for page_id in all_pages:
                        f.write(f"{page_id}\n")
                print(f"Saved {len(all_pages)} page IDs to cache file")
            except Exception as e:
                print(f"Error saving page IDs to cache: {e}")
        
        export_pages(all_pages, export_path)

    @classmethod
    def from_json(cls, data: JsonResponse) -> "Space":
        return cls(
            key=data.get("key", ""),
            name=data.get("name", ""),
            description=data.get("description", {}).get("plain", {}).get("value", ""),
            homepage=data.get("homepage", {}).get("id"),
        )

    @classmethod
    @functools.lru_cache(maxsize=100)
    def from_key(cls, space_key: str) -> "Space":
        return cls.from_json(cast(JsonResponse, confluence.get_space(space_key, expand="homepage")))


class Label(BaseModel):
    id: str
    name: str
    prefix: str

    @classmethod
    def from_json(cls, data: JsonResponse) -> "Label":
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            prefix=data.get("prefix", ""),
        )


class Document(BaseModel):
    title: str
    space: Space
    ancestors: list[int]

    @property
    def _template_vars(self) -> dict[str, str]:
        return {
            "space_key": sanitize_filename(self.space.key),
            "space_name": sanitize_filename(self.space.name),
            "homepage_id": str(self.space.homepage) if self.space.homepage is not None else "",
            "homepage_title": sanitize_filename(Page.from_id(self.space.homepage).title) if self.space.homepage is not None else "",
            "ancestor_ids": "/".join(str(a) for a in self.ancestors),
            "ancestor_titles": "/".join(
                sanitize_filename(self._get_safe_ancestor_title(a)) for a in self.ancestors
            )
        }

    def _get_safe_ancestor_title(self, ancestor_id: int) -> str:
        """Get the title of an ancestor page safely, returning 'N/A' if the page cannot be accessed.
        
        This specifically handles 403 Forbidden errors which may occur when a page is a draft
        or outside the Personal Access Token scope.
        """
        try:
            return Page.from_id(ancestor_id).title
        except HTTPError as e:
            if e.response.status_code == 403:
                return "N/A"
            raise  # Re-raise other HTTP errors


class Attachment(Document):
    id: str
    file_size: int
    media_type: str
    media_type_description: str
    file_id: str
    collection_name: str
    download_link: str
    comment: str

    @property
    def extension(self) -> str:
        if self.comment == "draw.io diagram" and self.media_type == "application/vnd.jgraph.mxfile":
            return ".drawio"
        if self.comment == "draw.io preview" and self.media_type == "image/png":
            return ".drawio.png"

        return mimetypes.guess_extension(self.media_type) or ""

    @property
    def filename(self) -> str:
        return f"{self.file_id}{self.extension}"

    @property
    def _template_vars(self) -> dict[str, str]:
        return {
            **super()._template_vars,
            "attachment_id": str(self.id),
            "attachment_title": sanitize_filename(self.title),
            "attachment_file_id": sanitize_filename(self.file_id),
            "attachment_extension": self.extension,
        }

    @property
    def export_path(self) -> Path:
        filepath_template = Template(converter_settings.attachment_path.replace("{", "${"))
        return Path(filepath_template.safe_substitute(self._template_vars))

    @classmethod
    def from_json(cls, data: JsonResponse) -> "Attachment":
        extensions = data.get("extensions", {})
        container = data.get("container", {})
        return cls(
            id=data.get("id", ""),
            title=data.get("title", ""),
            space=Space.from_key(data.get("_expandable", {}).get("space", "").split("/")[-1]),
            file_size=extensions.get("fileSize", 0),
            media_type=extensions.get("mediaType", ""),
            media_type_description=extensions.get("mediaTypeDescription", ""),
            file_id=extensions.get("fileId", ""),
            collection_name=extensions.get("collectionName", ""),
            download_link=data.get("_links", {}).get("download", ""),
            comment=extensions.get("comment", ""),
            ancestors=[
                *[ancestor.get("id") for ancestor in container.get("ancestors", [])],
                container.get("id"),
            ][1:],
        )

    def export(self, export_path: StrPath) -> None:
        filepath = Path(export_path) / self.export_path
        if filepath.exists():
            return

        try:
            response = confluence._session.get(str(confluence.url + self.download_link))
            response.raise_for_status()  # Raise error if request fails
        except HTTPError:
            print(f"There is no attachment with title '{self.title}'. Skipping export.")
            return

        save_file(
            filepath,
            response.content,
        )


class Page(Document):
    id: int
    body: str
    body_export: str
    editor2: str
    labels: list["Label"]
    attachments: list["Attachment"]

    @property
    def descendants(self) -> list[int]:
        # Use CQL search instead of descendant endpoint due to a bug in Confluence 8.5.20
        # that causes HTTP 500 errors when using the descendant endpoint
        url = "rest/api/search"
        cql_query = f"ancestor={self.id} AND type=page"
        try:
            response = cast(JsonResponse, confluence.get(url, params={"cql": cql_query, "limit": 10000}))
        except HTTPError as e:
            if e.response.status_code == 404:  # noqa: PLR2004
                # Raise ApiError as the documented reason is ambiguous
                msg = (
                    "There is no content with the given id, "
                    "or the calling user does not have permission to view the content"
                )
                raise ApiError(msg, reason=e) from e

            raise

        # The search API returns results with a different structure than the descendant endpoint
        return [page.get("content", {}).get("id") for page in response.get("results", [])]

    @property
    def _template_vars(self) -> dict[str, str]:
        return {
            **super()._template_vars,
            "page_id": str(self.id),
            "page_title": sanitize_filename(self.title),
        }

    @property
    def export_path(self) -> Path:
        filepath_template = Template(converter_settings.page_path.replace("{", "${"))
        return Path(filepath_template.safe_substitute(self._template_vars))

    @property
    def html(self) -> str:
        match converter_settings.markdown_style:
            case "GFM":
                return f"<h1>{self.title}</h1>{self.body}"
            case "Obsidian":
                return self.body
            case _:
                msg = f"Invalid markdown style: {converter_settings.markdown_style}"
                raise ValueError(msg)

    @property
    def markdown(self) -> str:
        return self.Converter(self).markdown

    def export(self, export_path: StrPath) -> None:
        if DEBUG:
            self.export_body(export_path)
        self.export_markdown(export_path)
        self.export_attachments(export_path)

    def export_with_descendants(self, export_path: StrPath) -> None:
        export_pages([self.id, *self.descendants], export_path)

    def export_body(self, export_path: StrPath) -> None:
        soup = BeautifulSoup(self.html, "html.parser")
        save_file(
            Path(export_path) / self.export_path.parent / f"{self.export_path.stem}_body_view.html",
            str(soup.prettify()),
        )
        soup = BeautifulSoup(self.body_export, "html.parser")
        save_file(
            Path(export_path)
            / self.export_path.parent
            / f"{self.export_path.stem}_body_export_view.html",
            str(soup.prettify()),
        )
        save_file(
            Path(export_path)
            / self.export_path.parent
            / f"{self.export_path.stem}_body_editor2.xml",
            str(self.editor2),
        )

    def export_markdown(self, export_path: StrPath) -> None:
        save_file(
            Path(export_path) / self.export_path,
            self.markdown,
        )

    def export_attachments(self, export_path: StrPath) -> None:
        for attachment in self.attachments:
            if (
                attachment.filename.endswith(".drawio")
                and f"diagramName={attachment.title}" in self.body
            ):
                attachment.export(export_path)
                continue
            if (
                attachment.filename.endswith(".drawio.png")
                and attachment.title.replace(" ", "%20") in self.body_export
            ):
                attachment.export(export_path)
                continue
            if attachment.file_id in self.body:
                attachment.export(export_path)
                continue

    def get_attachment_from_element(self, el: Tag) -> Attachment:
        file_id = el.get("data-media-id")
        if file_id:
            attachment = self.page.get_attachment_by_file_id(str(file_id))
        else:
            id = el.get("data-linked-resource-id")
            container_id = el.get("data-linked-resource-container-id")
            if not id or not container_id:
                return ""
            image_container_page = Page.from_id(str(container_id))
            if not image_container_page:
                return ""
            attachment = image_container_page.get_attachment_by_id(str(id))
        return attachment

    def get_attachment_by_id(self, id: str) -> Attachment:
        return next(attachment for attachment in self.attachments if attachment.id == id)

    def get_attachment_by_file_id(self, file_id: str) -> Attachment:
        return next(attachment for attachment in self.attachments if attachment.file_id == file_id)

    def get_attachments_by_title(self, title: str) -> list[Attachment]:
        return [attachment for attachment in self.attachments if attachment.title == title]

    @classmethod
    def from_json(cls, data: JsonResponse) -> "Page":
        attachments = cast(
            JsonResponse,
            confluence.get_attachments_from_content(
                data.get("id", 0), limit=1000, expand="container.ancestors"
            ),
        )
        return cls(
            id=data.get("id", 0),
            title=data.get("title", ""),
            space=Space.from_key(data.get("_expandable", {}).get("space", "").split("/")[-1]),
            body=data.get("body", {}).get("view", {}).get("value", ""),
            body_export=data.get("body", {}).get("export_view", {}).get("value", ""),
            editor2=data.get("body", {}).get("editor2", {}).get("value", ""),
            labels=[
                Label.from_json(label)
                for label in data.get("metadata", {}).get("labels", {}).get("results", [])
            ],
            attachments=[
                Attachment.from_json(attachment) for attachment in attachments.get("results", [])
            ],
            ancestors=[ancestor.get("id") for ancestor in data.get("ancestors", [])][1:],
        )

    @classmethod
    @functools.lru_cache(maxsize=1000)
    def from_id(cls, page_id: int) -> "Page":
        return cls.from_json(
            cast(
                JsonResponse,
                confluence.get_page_by_id(
                    page_id,
                    expand="body.view,body.export_view,body.editor2,metadata.labels,"
                    "metadata.properties,ancestors",
                ),
            )
        )

    class Converter(TableConverter, MarkdownConverter):
        """Create a custom MarkdownConverter for Confluence HTML to Markdown conversion."""

        # TODO Support table captions
        # TODO Support figure captions (934379624)

        # FIXME Potentially the REST API timesout - retry?

        # Advanced/Future features:
        # TODO Support badges via https://shields.io/badges/static-badge
        # TODO Read version by version and commit in git using change comment and user info

        # TODO what to do with page comments?
        # Insert using CriticMarkup: https://github.com/CriticMarkup/CriticMarkup-toolkit
        # There is also a plugin for Obsidian supporting CriticMarkup: https://github.com/Fevol/obsidian-criticmarkup/tree/main

        class Options(MarkdownConverter.DefaultOptions):
            bullets = "-"
            heading_style = ATX
            macros_to_ignore: Set[str] = frozenset(["qc-read-and-understood-signature-box"])
            front_matter_indent = 2

        def __init__(self, page: "Page", **options) -> None:  # noqa: ANN003
            super().__init__(**options)
            self.page = page
            self.page_properties = {}

        @property
        def markdown(self) -> str:
            md_body = self.convert(self.page.html)
            match converter_settings.markdown_style:
                case "GFM":
                    return f"{self.front_matter}\n{self.breadcrumbs}\n{md_body}\n"
                case "Obsidian":
                    return f"{self.front_matter}\n{md_body}\n"
                case _:
                    msg = f"Invalid markdown style: {converter_settings.markdown_style}"
                    raise ValueError(msg)
            return None

        @property
        def front_matter(self) -> str:
            indent = self.options["front_matter_indent"]
            self.set_page_properties(tags=self.labels)

            if not self.page_properties:
                return ""

            yml = yaml.dump(self.page_properties, indent=indent).strip()
            # Indent the root level list items
            yml = re.sub(r"^( *)(- )", r"\1" + " " * indent + r"\2", yml, flags=re.MULTILINE)
            return f"---\n{yml}\n---\n"

        @property
        def breadcrumbs(self) -> str:
            return (
                " > ".join([self.convert_page_link(ancestor) for ancestor in self.page.ancestors])
                + "\n"
            )

        @property
        def labels(self) -> list[str]:
            return [f"#{label.name}" for label in self.page.labels]

        def set_page_properties(self, **props: list[str] | str | None) -> None:
            for key, value in props.items():
                if value:
                    self.page_properties[sanitize_key(key)] = value

        def convert_page_properties(
            self, el: BeautifulSoup, text: str, parent_tags: list[str]
        ) -> None:
            # TODO can this be queries via REST API instead?

            rows = [
                cast(list[Tag], tr.find_all(["th", "td"]))
                for tr in cast(list[Tag], el.find_all("tr"))
                if tr
            ]
            if not rows:
                return

            props = {
                row[0].get_text(strip=True): self.convert(str(row[1])).strip()
                for row in rows
                if len(row) == 2  # noqa: PLR2004
            }

            self.set_page_properties(**props)

        def convert_alert(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            """Convert Confluence info macros to Markdown GitHub style alerts.

            GitHub specific alert types: https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax#alerts
            """
            alert_type_map = {
                "info": "IMPORTANT",
                "panel": "NOTE",
                "tip": "TIP",
                "note": "WARNING",
                "warning": "CAUTION",
            }

            alert_type = alert_type_map.get(str(el["data-macro-name"]), "NOTE")

            blockquote = super().convert_blockquote(el, text, parent_tags)
            return f"\n> [!{alert_type}]{blockquote}"

        def convert_div(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:  # noqa: PLR0911
            # Handle Confluence macros
            if el.has_attr("data-macro-name"):
                if el["data-macro-name"] in self.options["macros_to_ignore"]:
                    return ""
                if el["data-macro-name"] in ["panel", "info", "note", "tip", "warning"]:
                    return self.convert_alert(el, text, parent_tags)
                if el["data-macro-name"] == "details":
                    self.convert_page_properties(el, text, parent_tags)
                if el["data-macro-name"] == "drawio":
                    return self.convert_drawio(el, text, parent_tags)
                if el["data-macro-name"] == "scroll-ignore":
                    return self.convert_hidden_content(el, text, parent_tags)
                if el["data-macro-name"] == "toc":
                    return self.convert_toc(el, text, parent_tags)
                if el["data-macro-name"] == "jira":
                    return self.convert_jira_table(el, text, parent_tags)
            if "columnLayout" in str(el.get("class", "")):
                return self.convert_column_layout(el, text, parent_tags)

            return super().convert_div(el, text, parent_tags)

        def convert_span(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            if el.has_attr("data-macro-name"):
                if el["data-macro-name"] == "jira":
                    return self.convert_jira_issue(el, text, parent_tags)

            return text

        def convert_column_layout(
            self, el: BeautifulSoup, text: str, parent_tags: list[str]
        ) -> str:
            cells = el.find_all("div", {"class": "cell"})

            if len(cells) < 2:  # noqa: PLR2004
                return super().convert_div(el, text, parent_tags)

            html = f"<table><tr>{''.join([f'<td>{cell!s}</td>' for cell in cells])}</tr></table>"

            return self.convert_table(BeautifulSoup(html, "html.parser"), text, parent_tags)

        def convert_jira_table(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            jira_tables = BeautifulSoup(self.page.body_export, "html.parser").find_all(
                "div", {"class": "jira-table"}
            )

            if len(jira_tables) == 0:
                print("No Jira table found. Ignoring.")
                return text

            if len(jira_tables) > 1:
                print("Multiple Jira tables are not supported. Ignoring.")
                return text

            return self.process_tag(jira_tables[0], parent_tags)

        def convert_toc(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            tocs = BeautifulSoup(self.page.body_export, "html.parser").find_all(
                "div", {"class": "toc-macro"}
            )

            if len(tocs) == 0:
                print("Could not find TOC macro. Ignoring.")
                return text

            if len(tocs) > 1:
                print("Multiple TOC macros are not supported. Ignoring.")
                return text

            return self.process_tag(tocs[0], parent_tags)

        def convert_hidden_content(
            self, el: BeautifulSoup, text: str, parent_tags: list[str]
        ) -> str:
            content = super().convert_p(el, text, parent_tags)
            return f"\n<!--{content}-->\n"

        def convert_jira_issue(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            issue_key = el.get("data-jira-key")
            link = cast(BeautifulSoup, el.find("a", {"class": "jira-issue-key"}))
            if not issue_key:
                return self.process_tag(link, parent_tags)
            if not link:
                return text

            try:
                issue = JiraIssue.from_key(str(issue_key))
                return f"[[{issue.key}] {issue.summary}]({link.get('href')})"
            except HTTPError:
                return f"[[{issue_key}]]({link.get('href')})"

        def convert_pre(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            if not text:
                return ""

            code_language = ""
            if el.has_attr("data-syntaxhighlighter-params"):
                match = re.search(r"brush:\s*([^;]+)", str(el["data-syntaxhighlighter-params"]))
                if match:
                    code_language = match.group(1)

            return f"\n\n```{code_language}\n{text}\n```\n\n"

        def convert_sub(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            return f"<sub>{text}</sub>"

        def convert_sup(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            """Convert superscript to Markdown footnotes."""
            if el.previous_sibling is None:
                return f"[^{text}]:"  # Footnote definition
            return f"[^{text}]"  # f"<sup>{text}</sup>"

        def convert_a(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:  # noqa: PLR0911
            if "user-mention" in str(el.get("class")):
                return self.convert_user(el, text, parent_tags)
            if "createpage.action" in str(el.get("href")) or "createlink" in str(el.get("class")):
                if fallback := BeautifulSoup(self.page.editor2, "html.parser").find(
                    "a", string=text
                ):
                    return self.convert_a(fallback, text, parent_tags)  # type: ignore -
                return f"[[{text}]]"
            if "page" in str(el.get("data-linked-resource-type")):
                page_id = str(el.get("data-linked-resource-id", ""))
                if page_id and page_id != "null":
                    return self.convert_page_link(int(page_id), el)
            if "attachment" in str(el.get("data-linked-resource-type")):
                return self.convert_attachment_link(el, text, parent_tags)
            if match := re.search(r"/wiki/.+?/pages/(\d+)", str(el.get("href", ""))):
                page_id = match.group(1)
                return self.convert_page_link(int(page_id), el)
            if match := re.search(r"/pages/viewpage.action\?pageId=(\d+)", str(el.get("href", ""))):
                page_id = match.group(1)
                return self.convert_page_link(int(page_id), el)
            if match := re.search(r"/display/(\w+)/([^/]+)(?:\+|$)", str(el.get("href", ""))):
                space_key = match.group(1)
                page_name = match.group(2).replace("+", " ")
                page_id = get_page_id_by_space_and_name(space_key, page_name)
                if page_id:
                    return self.convert_page_link(int(page_id), el)
                # Fallback to default link handling if page not found
                return f"[{text}]({el.get('href', '')})"
            if str(el.get("href", "")).startswith("#"):
                # Handle heading links
                return f"[{text}](#{sanitize_key(text, '-')})"

            return super().convert_a(el, text, parent_tags)

        def convert_page_link(self, page_id: int, el: BeautifulSoup = None) -> str:
            if not page_id:
                msg = "Page link does not have valid page_id."
                raise ValueError(msg)

            page = Page.from_id(page_id)
            relpath = os.path.relpath(page.export_path, self.page.export_path.parent)

            link_content = page.title
            if el is not None:
                children = list(el.children)
                if children:
                    # Check if first child is a NavigableString (text node)
                    if isinstance(children[0], NavigableString):
                        link_content = children[0]
                    else:
                        link_content = self.process_tag(children[0], ["a"])
                else:
                    link_content = el.string

            return f"[{link_content}]({relpath.replace(' ', '%20')})"

        def convert_attachment_link(
            self, el: BeautifulSoup, text: str, parent_tags: list[str]
        ) -> str:
            attachment = self.page.get_attachment_from_element(el)
            relpath = os.path.relpath(attachment.export_path, self.page.export_path.parent)
            return f"[{attachment.title}]({relpath.replace(' ', '%20')})"

        def convert_time(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            if el.has_attr("datetime"):
                return f"{el['datetime']}"  # TODO convert to date format?

            return f"{text}"

        def convert_user(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            return f"{text.removesuffix('(Unlicensed)').removesuffix('(Deactivated)').strip()}"

        def convert_li(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            md = super().convert_li(el, text, parent_tags)
            bullet = self.options["bullets"][0]

            # Convert Confluence task lists to GitHub task lists
            if el.has_attr("data-inline-task-id"):
                is_checked = el.has_attr("class") and "checked" in el["class"]
                return md.replace(f"{bullet} ", f"{bullet} {'[x]' if is_checked else '[ ]'} ", 1)

            return md

        def convert_img(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            # First try to get attachment from the element
            attachment = self.page.get_attachment_from_element(el)
            if attachment:
                relpath = os.path.relpath(attachment.export_path, self.page.export_path.parent)
                el["src"] = relpath.replace(" ", "%20")
                if "_inline" in parent_tags:
                    parent_tags.remove("_inline")  # Always show images.
                return super().convert_img(el, text, parent_tags)
            
            # Handle direct image URLs (emoticons, etc.)
            src = el.get("src", "")
            if src:
                # Get the global output path
                output_path = Path(converter_settings.output_root_path)

                assets_dir = output_path / "assets"
                assets_dir.mkdir(parents=True, exist_ok=True)
                
                # Check if this is an inline base64 image
                if src.startswith("data:"):
                    try:
                        # Parse the mime type and base64 data
                        mime_type_match = re.match(r"data:([^;]+);base64,(.+)", src)
                        if mime_type_match:
                            mime_type = mime_type_match.group(1)
                            base64_data = mime_type_match.group(2)
                            
                            # Determine file extension from mime type
                            extension = mimetypes.guess_extension(mime_type) or ".bin"
                            
                            # Hash the base64 data
                            data_hash = hashlib.md5(base64_data.encode()).hexdigest()
                            
                            # Set the file path
                            file_path = assets_dir / f"{data_hash}{extension}"
                            
                            # Save the file if it doesn't exist
                            if not file_path.exists():
                                try:
                                    # Decode and save the file
                                    image_data = base64.b64decode(base64_data)
                                    save_file(file_path, image_data)
                                except Exception as e:
                                    if DEBUG:
                                        print(f"Error saving base64 image: {e}")
                                    return ""
                            
                            # Update src to point to local file - use relative path from page export location
                            rel_img_path = os.path.relpath(file_path, output_path / self.page.export_path.parent)
                            cloned_el = copy.copy(el)
                            cloned_el["src"] = rel_img_path.replace(" ", "%20")
                            if "_inline" in parent_tags:
                                parent_tags.remove("_inline")  # Always show images.
                            return super().convert_img(cloned_el, text, parent_tags)
                    except Exception as e:
                        if DEBUG:
                            print(f"Error processing base64 image: {e}")
                        return ""
                
                # Calculate a hash from the original src value
                src_hash = hashlib.md5(src.encode()).hexdigest()
                
                # Look for existing files with this hash (any extension)
                existing_files = list(assets_dir.glob(f"{src_hash}.*"))
                
                if existing_files:
                    # Use the first existing file with this hash
                    file_path = existing_files[0]
                else:
                    # File not found, download it
                    try:
                        # Download the file
                        url = confluence.url + src if src.startswith("/") else src
                        parsed_url = urlparse(url)
                        if parsed_url.netloc.startswith('jira'):
                            # Use standard HTTP client with Jira cookie for Jira URLs
                            jira_cookie = os.environ.get('JIRA_COOKIE')
                            headers = {'Cookie': jira_cookie} if jira_cookie else {}
                            response = requests.get(url, headers=headers)
                            response.raise_for_status()
                            
                            # Check if response is HTML (consider it failed request)
                            content_type = response.headers.get('Content-Type', '')
                            if 'text/html' in content_type.lower():
                                if DEBUG:
                                    print(f"Error: Jira URL {url} returned HTML response instead of an image")
                                raise Exception("HTML response received instead of an image")
                        else:
                            # Use existing confluence session for other URLs
                            response = confluence._session.get(url)
                            response.raise_for_status()
                        
                        # Determine file extension from response content type
                        content_type = response.headers.get('Content-Type', '')
                        extension = mimetypes.guess_extension(content_type) or '.bin'
                        
                        # If extension wasn't resolved, try to extract it from the URL
                        if extension == '.bin':
                            # Extract extension from URL if present
                            url_path = urlparse(url).path
                            url_extension = os.path.splitext(url_path)[1].lower()
                            
                            # List of known image extensions
                            known_image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.bmp', '.tiff', '.ico']
                            
                            if url_extension and url_extension in known_image_extensions:
                                extension = url_extension
                            else:
                                # Log error if extension couldn't be resolved
                                print(f"Error: Could not determine file extension for {url}. Using .bin")
                        
                        # Set file path with hash and extension
                        file_path = assets_dir / f"{src_hash}{extension}"
                        
                        # Save the file
                        save_file(file_path, response.content)
                    except Exception as e:
                        if DEBUG:
                            print(f"Error downloading image from {src}: {e}")
                        return ""
                
                # Update src to point to local file - use relative path from page export location
                rel_img_path = os.path.relpath(file_path, output_path / self.page.export_path.parent)
                cloned_el = copy.copy(el)
                cloned_el["src"] = rel_img_path.replace(" ", "%20")
                if "_inline" in parent_tags:
                    parent_tags.remove("_inline")  # Always show images.
                return super().convert_img(cloned_el, text, parent_tags)
            
            return ""

        def convert_drawio(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            if match := re.search(r"\|diagramName=(.+?)\|", str(el)):
                drawio_name = match.group(1)
                preview_name = f"{drawio_name}.png"
                drawio_attachments = self.page.get_attachments_by_title(drawio_name)
                preview_attachments = self.page.get_attachments_by_title(preview_name)

                if not drawio_attachments or not preview_attachments:
                    return f"\n<!-- Drawio diagram `{drawio_name}` not found -->\n\n"

                drawio_relpath = os.path.relpath(
                    drawio_attachments[0].export_path,
                    self.page.export_path.parent,
                )
                preview_relpath = os.path.relpath(
                    preview_attachments[0].export_path,
                    self.page.export_path.parent,
                )

                drawio_image_embedding = f"![{drawio_name}]({preview_relpath.replace(' ', '%20')})"
                drawio_link = f"[{drawio_image_embedding}]({drawio_relpath.replace(' ', '%20')})"
                return f"\n{drawio_link}\n\n"

            return ""

        def convert_table(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
            if el.has_attr("class") and "metadata-summary-macro" in el["class"]:
                return self.convert_page_properties_report(el, text, parent_tags)

            return super().convert_table(el, text, parent_tags)

        def convert_page_properties_report(
            self, el: BeautifulSoup, text: str, parent_tags: list[str]
        ) -> str:
            # TODO can this be queries via REST API instead?
            # api.cql('label = "curated-dataset" and space = STRUCT and parent = 688816133', expand='metadata.properties')
            # data-macro-id="5836d104-f9e9-44cf-9d05-e332b86275c0"
            # https://developer.atlassian.com/cloud/confluence/rest/v1/api-group-content---macro-body/#api-wiki-rest-api-content-id-history-version-macro-id-macroid-get
            # Find out how to fetch the macro content

            # TODO instead use markdown integrated front matter properties query

            data_cql = el.get("data-cql")
            if not data_cql:
                return ""
            soup = BeautifulSoup(self.page.body_export, "html.parser")
            table = soup.find("table", {"data-cql": data_cql})
            if not table:
                return ""
            return super().convert_table(table, "", parent_tags)  # type: ignore -


def export_page(page_id: int, output_path: StrPath) -> None:
    """Export a Confluence page to Markdown.

    Args:
        page_id: The page id.
        output_path: The output path.
    """
    # Set the global output path
    converter_settings.output_root_path = output_path
    
    page = Page.from_id(page_id)
    page.export(output_path)


def export_pages(page_ids: list[int], output_path: StrPath) -> None:
    """Export a list of Confluence pages to Markdown.

    Args:
        page_ids: List of pages to export.
        output_path: The output path.
    """
    # Set the global output path
    converter_settings.output_root_path = output_path
    
    # Create cache directory if it doesn't exist
    output_path_obj = Path(output_path)
    cache_dir = output_path_obj / ".cache"
    os.makedirs(cache_dir, exist_ok=True)
    processed_pages_file = cache_dir / "processed_pages.txt"
    
    # Read the list of already processed pages
    processed_pages = set()
    if processed_pages_file.exists():
        try:
            with open(processed_pages_file, "r") as f:
                processed_pages = {line.strip() for line in f}
            print(f"Found {len(processed_pages)} previously processed pages")
        except Exception as e:
            print(f"Error reading processed pages file: {e}")
    
    # Filter out already processed pages
    pages_to_process = [page_id for page_id in page_ids if page_id not in processed_pages]
    if len(pages_to_process) < len(page_ids):
        print(f"Skipping {len(page_ids) - len(pages_to_process)} already processed pages")
    
    # Process remaining pages
    for page_id in (pbar := tqdm(pages_to_process, smoothing=0.05)):
        pbar.set_postfix_str(f"Exporting page {page_id}")
        try:
            export_page(page_id, output_path)
            
            # Append the processed page ID to the file
            with open(processed_pages_file, "a") as f:
                f.write(f"{page_id}\n")
        except Exception as e:
            print(f"Error exporting page {page_id}: {e}")


@functools.lru_cache(maxsize=10000)
def get_page_id_by_space_and_name(space_key: str, page_name: str) -> Optional[int]:
    """Get a page ID by space key and page name.

    Args:
        space_key: The key of the space containing the page
        page_name: The page name/title to look for

    Returns:
        Page ID if found, None otherwise
    """
    try:
        # Search for content using CQL (Confluence Query Language)
        results = confluence.cql(
            f'space.key="{space_key}" AND type=page AND title="{page_name}"',
            limit=1
        )

        if results and results.get("results") and len(results["results"]) > 0:
            return int(results["results"][0]["content"]["id"])
        return None
    except Exception as e:
        if DEBUG:
            print(f"Error finding page by name: {e}")
        return None
