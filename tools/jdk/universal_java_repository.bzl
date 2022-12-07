_TOOLS = ["java", "javac"]
_DUMMIES = ["lib/jrt-fs.jar", "modules/so_that_dir_exists"]

def _get_build_file(ctx):
    tools = "\n".join(["\"bin/{}\",".format(t) for t in _TOOLS])
    dummies = "\n".join(["\"{}\",".format(d) for d in _DUMMIES])
    return """
load("@rules_java//java:defs.bzl", "java_runtime")

java_runtime(
    name = "jdk",
    srcs = [
        "@{linux_repo}//:jdk",
        "@{macos_repo}//:jdk",
        {tools}
        {dummies}
    ],
)
""".format(linux_repo = ctx.attr.linux_repo, macos_repo = ctx.attr.macos_repo, tools = tools, dummies = dummies)

def _get_tool_wrapper(ctx, tool):
    return """#!/bin/bash
external=$(dirname -- "${{BASH_SOURCE[0]}}")/../..
if [[ "$OSTYPE" == linux* ]]; then
    exec "$external/{linux_repo}/bin/{tool}" "$@"
elif [[ "$OSTYPE" == darwin* ]]; then
    exec "$external/{macos_repo}/bin/{tool}" "$@"
else
    echo "Unknown platform $OSTYPE" >&2
    exit 1
fi
""".format(linux_repo = ctx.attr.linux_repo, macos_repo = ctx.attr.macos_repo, tool = tool)

def _universal_java_repo_impl(ctx):
    ctx.file("WORKSPACE", "workspace(name = \"{name}\")\n".format(name = ctx.name))
    ctx.file("BUILD", _get_build_file(ctx))

    for tool in _TOOLS:
        ctx.file("bin/{tool}".format(tool = tool), _get_tool_wrapper(ctx, tool), executable = True)
    for dummy in _DUMMIES:
        ctx.file(dummy)

_universal_java_repo = repository_rule(
    local = True,
    implementation = _universal_java_repo_impl,
    attrs = {
        "linux_repo": attr.string(),
        "macos_repo": attr.string(),
    },
)

def _toolchain_config_impl(ctx):
    ctx.file("WORKSPACE", "workspace(name = \"{name}\")\n".format(name = ctx.name))
    ctx.file("BUILD.bazel", ctx.attr.build_file)

_toolchain_config = repository_rule(
    local = True,
    implementation = _toolchain_config_impl,
    attrs = {
        "build_file": attr.string(),
    },
)

def universal_java_repository(name, version, target_compatible_with = None, prefix = "remotejdk", **kwargs):
    _universal_java_repo(
        name = name,
        linux_repo = "{prefix}{version}_linux".format(
            prefix = prefix,
            version = version,
        ),
        macos_repo = "{prefix}{version}_macos_aarch64".format(
            prefix = prefix,
            version = version,
        ),
    )
    _toolchain_config(
        name = name + "_toolchain_config_repo",
        build_file = """
config_setting(
    name = "prefix_version_setting",
    values = {{"java_runtime_version": "{prefix}_{version}"}},
    visibility = ["//visibility:private"],
)
config_setting(
    name = "version_setting",
    values = {{"java_runtime_version": "{version}"}},
    visibility = ["//visibility:private"],
)
alias(
    name = "version_or_prefix_version_setting",
    actual = select({{
        ":version_setting": ":version_setting",
        "//conditions:default": ":prefix_version_setting",
    }}),
    visibility = ["//visibility:private"],
)
toolchain(
    name = "toolchain",
    target_compatible_with = {target_compatible_with},
    target_settings = [":version_or_prefix_version_setting"],
    toolchain_type = "@bazel_tools//tools/jdk:runtime_toolchain_type",
    toolchain = "{toolchain}",
)
""".format(
            prefix = prefix,
            version = version,
            target_compatible_with = target_compatible_with,
            toolchain = "@{repo}//:jdk".format(repo = name),
        ),
    )
    native.register_toolchains("@" + name + "_toolchain_config_repo//:toolchain")
