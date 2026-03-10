import asyncio
import random
from nonebot import (
    on_command,
    require,
    logger,
    on_notice,
    get_driver
)
from nonebot.adapters.onebot.v11 import NoticeEvent
try:
    require("nonebot_plugin_apscheduler")
    from nonebot_plugin_apscheduler import scheduler
except Exception:
    scheduler = None


plugins = [
    "nonebot_plugin_localstore",
    "nonebot_plugin_waiter"
]

for plugin in plugins:
    require(plugin)

from pathlib import Path
from .config import plugin_config as _config
from .model import DeleteFile
from nonebot.permission import SUPERUSER
from nonebot_plugin_waiter import suggest
from nonebot_plugin_localstore import get_plugin_data_dir
from nonebot.adapters.onebot.v11 import (
    GroupMessageEvent,
    Bot
)


from nonebot.plugin import PluginMetadata
from .config import Config
__plugin_meta__ = PluginMetadata(
    name="群文件管理",
    description="基于Lagrange.OneBot & NoneBot2的群文件管理插件",
    usage="清理群文件：清理群文件\n备份群文件：备份群文件\n恢复群文件：恢复群文件\n文件整理：文件整理",
    type="application",
    homepage="https://github.com/zhongwen-4/nonebot-plugin-group-file-admin",
    supported_adapters={"~onebot.v11"},
    config= Config
)


del_file = on_command(
    "清理群文件",
    permission= SUPERUSER
)

copy_file = on_command(
    "备份群文件",
    permission= SUPERUSER
)

del_local_file = on_command(
    "清理本地文件",
    permission= SUPERUSER
)

recover_flie = on_command(
    "恢复群文件",
    permission= SUPERUSER
)

file_arrange = on_command(
    "文件整理",
    permission= SUPERUSER
)

auto_backup = on_command(
    "自动备份群文件",
    permission= SUPERUSER
)


@del_file.handle()
async def del_flie_handle(bot: Bot, event: GroupMessageEvent):
    option = await suggest(
        message= "注意！！！\n数据无价，请谨慎操作！\n是否确认删除群文件？",
        expect=["y", "n"]
    )
    option = str(option)
    delfile = DeleteFile(event.group_id)

    if option == "n":
        await del_file.finish("已取消")

    if option == "y":
        await del_file.send("开始删除群文件...")
        if _config.fa_del_model == 1:
            datas = await delfile.get_root_data(bot)

            for i in datas.files:
                await delfile.del_file(bot, i.file_id)
                await asyncio.sleep(random.uniform(1.0, 2.5))

            await del_file.finish("删除完成！")

        if _config.fa_del_model == 2:
            if not _config.fa_expand_name:
                await del_file.finish("请配置需要删除的文件后缀！")

            datas = await delfile.get_root_data(bot)

            for i in datas.files:
                if i.file_name.endswith(tuple(i for i in _config.fa_expand_name)):
                    await delfile.del_file(bot, i.file_id)
                    await asyncio.sleep(random.uniform(1.0, 2.5))

            for i in datas.folders:
                datas_ = await delfile.get_folder_data(bot, i.folder_id)
                await asyncio.sleep(random.uniform(0.5, 1.5))

                for file in datas_.files:
                    if file.file_name.endswith(tuple(i for i in _config.fa_expand_name)):
                        await delfile.del_file(bot, file.file_id)
                        await asyncio.sleep(random.uniform(1.0, 2.5))

            await del_file.finish("删除完成！")

        if _config.fa_del_model == 3:
            confirm = await suggest(
                "再次确认：你正在删除所有群文件，是否继续？",
                expect=["y", "n"]
            )
            confirm = str(confirm)

            if confirm == "n":
                await del_file.finish("已取消")

            if confirm == "y":
                await del_file.send("开始删除群文件...")

                files = await delfile.get_root_data(bot)
                for i in files.files:
                    await delfile.del_file(bot, i.file_id)
                    await asyncio.sleep(random.uniform(1.0, 2.5))

                for i in files.folders:
                    await delfile.del_folder(bot, i.folder_id)
                    await asyncio.sleep(random.uniform(1.0, 2.5))

                await del_file.finish("删除完成！")


@copy_file.handle()
async def copy_file_handle(bot: Bot, event: GroupMessageEvent):
    get_file_and_foler = DeleteFile(event.group_id)
    path = get_plugin_data_dir()

    await copy_file.send("开始备份群文件...")
    copys = await get_file_and_foler.get_root_data(bot)

    # 备份根目录文件
    for i in copys.files:
        await download_file(bot, event.group_id, i, path / str(event.group_id))

    # 备份文件夹内文件
    for i in copys.folders:
        # 修复逻辑问题：过滤掉被错误识别为子文件夹的根目录自身
        if i.folder_id in ["/", "", "0"] or i.folder_name in ["/", ""]:
            continue

        logger.info(f"正在备份文件夹：{i.folder_name}")
        await asyncio.sleep(random.uniform(0.5, 1.5))
        folder_data = await get_file_and_foler.get_folder_data(bot, i.folder_id)

        for files in folder_data.files:
            await download_file(bot, event.group_id, files, path / str(event.group_id) / i.folder_name)

    await copy_file.finish("备份完成！")


@recover_flie.handle()
async def recover_flie_handle(bot: Bot, event: GroupMessageEvent):
    path = Path(f"{get_plugin_data_dir()}/{event.group_id}")
    files= DeleteFile(event.group_id)
    data= await files.get_root_data(bot)
    folder_names = [i.folder_name for i in data.folders]

    confirm= await suggest(
        "你正在恢复所有群文件，是否继续？",
        expect=["y", "n"]
    )

    if confirm == "n":
        await recover_flie.finish("已取消")

    if confirm == "y":
        await recover_flie.send("开始恢复群文件...")

    if not path.exists():
        await recover_flie.finish("没有备份文件！")

    for item in path.iterdir():
        if item.is_dir():
            if item.name not in folder_names:
                logger.debug(f"文件夹不存在，正在创建文件夹：{item.name}")
                await bot.call_api(
                    "create_group_file_folder", group_id= event.group_id, name= item.name
                )
                logger.debug(f"创建文件夹成功：{item.name}")
                await asyncio.sleep(random.uniform(1.0, 2.5))

            data= await files.get_root_data(bot)
            folder_id = [i.folder_id for i in data.folders if i.folder_name == item.name]
            logger.debug(f"获取文件夹ID | {item} : {folder_id}")

            for file in item.glob("*"):
                logger.info(f"正在恢复{file}")
                print(file)
                await bot.call_api(
                    "upload_group_file", group_id= event.group_id, file= str(file), name= file.name, folder= folder_id[0]
                )
                logger.info(f"恢复完成：{file}")
                await asyncio.sleep(random.uniform(2.5, 5.0))

        if item.is_file():
            logger.info(f"正在恢复文件：{item}")
            await bot.call_api(
                "upload_group_file", group_id= event.group_id, file= str(item), name= item.name
            )
            logger.info(f"恢复完成：{item}")
            await asyncio.sleep(random.uniform(2.5, 5.0))

    await recover_flie.finish("恢复完成！")


@del_local_file.handle()
async def del_local_file_handle(event: GroupMessageEvent):
    import shutil

    path = Path(f"{get_plugin_data_dir()}/{event.group_id}")
    if not path.exists():
        await del_local_file.finish("没有备份文件！")

    await del_local_file.send("开始删除本地备份文件...")

    for item in path.iterdir():
        if item.is_dir():
            logger.info(f"正在删除文件夹：{item}")
            shutil.rmtree(item)
            logger.info(f"删除完成：{item}")

        if item.is_file():
            logger.info(f"正在删除文件：{item}")
            item.unlink()
            logger.info(f"删除完成：{item}")

    await del_local_file.finish("删除完成！")


@file_arrange.handle()
async def file_arrange_handle(bot: Bot, event: GroupMessageEvent):
    import re, time
    from collections import OrderedDict
    from nonebot.adapters.onebot.v11.exception import ActionFailed

    await file_arrange.send("开始整理群文件...")

    t = time.time()
    file = DeleteFile(event.group_id)
    data = await file.get_root_data(bot)
    file_expand = [re.findall(r"\.[^\.]+$", i.file_name)[0] for i in data.files]
    file_expand = list(OrderedDict.fromkeys(file_expand))


    for expand in file_expand:
        try:
            logger.info(f"创建文件夹：{expand[1:]}")
            await bot.call_api(
                "create_group_file_folder", group_id= event.group_id, name= expand[1:]
            )
            logger.info(f"文件夹 {expand[1:]} 创建完成")
            await asyncio.sleep(random.uniform(1.0, 2.0))
        except ActionFailed:
            logger.info(f"文件夹 {expand[1:]} 已存在，跳过创建文件夹")
            continue

    # 防风控极度致命处修复：在创建完所有文件夹后，统一获取一次最新目录，避免在内层循环发育成百上千次请求
    await asyncio.sleep(1.0)
    updated_root_data = await file.get_root_data(bot)

    for files in data.files:
        file_names = re.findall(r'(\S+)\.(\w+)', files.file_name)
        logger.info(f"获取文件名：{files.file_name}")
        for expand_name in file_names:
            for folder_name in updated_root_data.folders:
                if folder_name.folder_name == expand_name[1]:
                    logger.info(f"匹配文件夹完成: {files.file_name} -> {folder_name.folder_name}")
                    logger.debug(f"文件夹ID：{folder_name.folder_id} | 文件ID：{files.file_id}")
                    await bot.call_api(
                        "move_group_file", group_id= event.group_id, file_id= files.file_id, target_directory= folder_name.folder_id
                    )
                    logger.info(f"移动文件完成: {files.file_name} -> {folder_name.folder_name}")
                    await asyncio.sleep(random.uniform(1.5, 3.0))

    logger.info(f"整理完成，耗时：{time.time() - t}s")
    await file_arrange.finish("整理完成！")


@auto_backup.handle()
async def auto_backup_handle(bot: Bot, event: GroupMessageEvent):
    if not _config.fa_white_group_list:
        await auto_backup.finish("请配置需要备份的群号！")

    await auto_backup.send("开始自动备份群文件...")

    for group_id in _config.fa_white_group_list:
        await perform_group_backup(bot, group_id)
        await asyncio.sleep(random.uniform(3.0, 6.0))

    await auto_backup.finish("自动备份完成！")


async def perform_group_backup(bot: Bot, group_id: int):
    path = get_plugin_data_dir()
    logger.info(f"正在备份群：{group_id}")
    get_file_and_foler = DeleteFile(group_id)

    try:
        copys = await get_file_and_foler.get_root_data(bot)
    except Exception as e:
        logger.error(f"获取群 {group_id} 文件列表失败: {e}")
        return

    # Backup root files
    if not _config.fa_white_folder_list:
        for i in copys.files:
            await download_file(bot, group_id, i, path / str(group_id))

    # Backup folders
    for i in copys.folders:
        # 修复逻辑问题：过滤掉被错误识别为子文件夹的根目录自身
        if i.folder_id in ["/", "", "0"] or i.folder_name in ["/", ""]:
            continue

        if _config.fa_white_folder_list and i.folder_name not in _config.fa_white_folder_list:
            logger.debug(f"跳过文件夹：{i.folder_name}")
            continue

        logger.info(f"正在备份文件夹：{i.folder_name}")
        try:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            folder_data = await get_file_and_foler.get_folder_data(bot, i.folder_id)
            for files in folder_data.files:
                await download_file(bot, group_id, files, path / str(group_id) / i.folder_name)
        except Exception as e:
            logger.error(f"备份文件夹 {i.folder_name} 失败: {e}")


async def download_file(bot: Bot, group_id: int, file_info, parent_path: Path):
    """提取的通用文件下载函数"""
    import httpx

    parent_path.mkdir(parents=True, exist_ok=True)
    file_path = parent_path / file_info.file_name

    # Incremental check
    if file_path.exists():
        if file_path.stat().st_size == file_info.size:
            logger.debug(f"文件已存在且大小一致，跳过备份：{file_info.file_name}")
            return
        else:
            logger.info(f"文件已存在但大小不一致，重新备份：{file_info.file_name}")

    logger.info(f"正在准备备份文件：{file_info.file_name}")
    try:
        await asyncio.sleep(random.uniform(1.0, 2.5))

        # 超时放开，流式写入
        async with httpx.AsyncClient(timeout=None) as client:
            try:
                # Try to get file URL
                file_url = await bot.call_api(
                    "get_group_file_url", group_id=group_id, file_id=file_info.file_id
                )
                url = file_url.get("url")
            except Exception:
                # Fallback or specific error handling if needed
                logger.warning(f"无法获取文件下载链接：{file_info.file_name}")
                return

            if not url:
                return

            async with client.stream("GET", url) as response:
                response.raise_for_status()
                with open(file_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
            logger.info(f"备份完成：{file_info.file_name}")
    except Exception as e:
        logger.error(f"备份文件 {file_info.file_name} 失败: {e}")


# Real-time backup listener
group_file_upload = on_notice(priority=5)

@group_file_upload.handle()
async def handle_group_file_upload(bot: Bot, event: NoticeEvent):
    # 检查是否为群文件上传事件
    if event.notice_type != "group_upload":
        return

    group_id = getattr(event, "group_id", None)

    # Check whitelist
    if _config.fa_white_group_list:
        if group_id not in _config.fa_white_group_list:
            return

    file_info = getattr(event, "file", None)
    if not file_info:
        return

    logger.info(f"检测到群 {group_id} 文件上传：{file_info.name}")

    try:
        file_id = getattr(file_info, "id", None)
        if file_id is None:
             file_id = file_info.get("id")

        file_name = getattr(file_info, "name", None)
        if file_name is None:
             file_name = file_info.get("name")

        file_size = getattr(file_info, "size", None)
        if file_size is None:
             file_size = file_info.get("size")

        class FileInfo:
            def __init__(self, id, name, size):
                self.file_id = id
                self.file_name = name
                self.size = size

        wrapped_info = FileInfo(file_id, file_name, file_size)
    except Exception as e:
        logger.error(f"获取文件信息失败: {str(e)}")
        return

    path = get_plugin_data_dir()
    files_helper = DeleteFile(group_id)

    try:
        await asyncio.sleep(random.uniform(1.0, 3.0))
        root_data = await files_helper.get_root_data(bot)
        found_folder_name = None
        is_in_root = False

        # 先检查文件是否直接上传到了根目录
        for f in root_data.files:
            if f.file_id == file_id:
                is_in_root = True
                break

        # 如果不在根目录，则去各个子文件夹里找
        if not is_in_root:
            for f in root_data.folders:
                # 过滤异常文件夹
                if f.folder_id in ["/", "", "0"] or f.folder_name in ["/", ""]:
                    continue

                await asyncio.sleep(random.uniform(0.2, 0.8))
                folder_files = await files_helper.get_folder_data(bot, f.folder_id)
                for ff in folder_files.files:
                    if ff.file_id == file_id:
                        found_folder_name = f.folder_name
                        break
                if found_folder_name:
                    break

        # 根据是否配置了白名单，结合刚刚找到的实际路径执行备份
        if _config.fa_white_folder_list:
            if found_folder_name and found_folder_name in _config.fa_white_folder_list:
                logger.info(f"文件 {file_name} 位于白名单文件夹 {found_folder_name}，开始备份")
                save_path = path / str(group_id) / found_folder_name
                await download_file(bot, group_id, wrapped_info, save_path)
            else:
                logger.info(f"文件 {file_name} 不在白名单文件夹中，跳过备份")
                return
        else:
            if found_folder_name:
                logger.info(f"文件 {file_name} 位于文件夹 {found_folder_name}，开始备份")
                save_path = path / str(group_id) / found_folder_name
            else:
                logger.info(f"文件 {file_name} 位于根目录，开始备份")
                save_path = path / str(group_id)

            await download_file(bot, group_id, wrapped_info, save_path)

    except Exception as e:
        logger.error(f"检查文件文件夹归属失败: {e}")
        return


# Startup hook
driver = get_driver()
@driver.on_bot_connect
async def _(bot: Bot):
    if not _config.fa_white_group_list:
        return

    logger.info("Bot连接成功，开始执行启动时群文件备份检查...")
    for group_id in _config.fa_white_group_list:
        await asyncio.sleep(random.uniform(5.0, 15.0))
        asyncio.create_task(perform_group_backup(bot, group_id))


# Scheduler
if scheduler:
    @scheduler.scheduled_job("cron", hour=2, minute=0, id="group_file_backup")
    async def scheduled_backup():
        logger.info("开始执行定时群文件备份...")
        import nonebot
        bots = nonebot.get_bots()
        for bot_id, bot in bots.items():
            try:
                group_list = await bot.get_group_list()
                for group in group_list:
                    group_id = group["group_id"]
                    await asyncio.sleep(random.uniform(5.0, 15.0))
                    asyncio.create_task(perform_group_backup(bot, group_id))
            except Exception as e:
                logger.error(f"执行定时备份时获取群列表失败: {e}")
