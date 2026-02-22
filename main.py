import os
import time
import uuid
import json
import asyncio
import aiohttp
import aiofiles
from io import BytesIO
from typing import Union, List, Tuple
from urllib.parse import urlparse
from pathlib import Path 
from PIL import Image as PILImage 

from astrbot.api.message_components import Image, Plain
from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import logger 

@register("mccloud_img", "随机图片", "安全引擎：支持合并转发、双重撤回与直链兜底。", "5.7.1")
class SetuPlugin(Star):
    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.cfg = config 
        
        self.cooldowns = {}
        self.cache_dir = os.path.join(os.getcwd(), "data", "temp_images")
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir, exist_ok=True)

    def _text(self, base_text: str) -> str:
        if self.cfg.get("catgirl_enable", False):
            suffix = self.cfg.get("catgirl_suffix", "喵~")
            return f"{base_text}{suffix}"
        return base_text

    def _clean_cooldowns(self):
        now = time.time()
        cooldown_time = self.cfg.get("cooldown", 10)
        self.cooldowns = {uid: t for uid, t in self.cooldowns.items() if now - t < cooldown_time}

    def _check_cooldown(self, user_id: str) -> float:
        self._clean_cooldowns()
        current_time = time.time()
        cooldown_time = self.cfg.get("cooldown", 10)
        if user_id in self.cooldowns:
            elapsed = current_time - self.cooldowns[user_id]
            if elapsed < cooldown_time:
                return cooldown_time - elapsed
        return 0

    def _is_safe_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname
            if not hostname: return False
            forbidden_hosts = ['localhost', '127.0.0.1', '::1', '0.0.0.0']
            if hostname in forbidden_hosts: return False
            if hostname.startswith('192.168.') or hostname.startswith('10.') or hostname.startswith('172.'):
                return False
            return True
        except Exception:
            return False

    def _extract_url_from_json(self, data: Union[dict, list]) -> str:
        if isinstance(data, list):
            for item in data:
                res = self._extract_url_from_json(item)
                if res: return res
        elif isinstance(data, dict):
            for key in ["original", "url_original", "url", "img", "image", "src", "link"]:
                if key in data and isinstance(data[key], str) and data[key].startswith("http"):
                    return data[key]
            for value in data.values():
                res = self._extract_url_from_json(value)
                if res: return res
        return ""

    def _compress_image(self, image_data: bytes) -> bytes:
        if not self.cfg.get("compress_enable", True): return image_data
        threshold_mb = self.cfg.get("compress_threshold", 5)
        if len(image_data) <= threshold_mb * 1024 * 1024: return image_data
        try:
            img = PILImage.open(BytesIO(image_data))
            if img.mode != 'RGB': img = img.convert('RGB')
            quality = self.cfg.get("compress_quality", 85)
            output_buffer = BytesIO()
            img.save(output_buffer, format='JPEG', quality=quality)
            return output_buffer.getvalue()
        except Exception:
            return image_data 

    async def _safe_fetch(self, session: aiohttp.ClientSession, url: str, max_size_mb: int = 20) -> Tuple[bytes, str, str]:
        if not self._is_safe_url(url): return b"", "", url
        try:
            async with session.get(url, allow_redirects=True, timeout=20) as response:
                if response.status != 200: return b"", "", url
                content_type = response.headers.get("Content-Type", "").lower()
                final_url = str(response.url)
                body = b""
                max_bytes = max_size_mb * 1024 * 1024
                while True:
                    chunk = await response.content.read(8192)
                    if not chunk: break
                    body += chunk
                    if len(body) > max_bytes: return b"", "", final_url
                return body, content_type, final_url
        except Exception: pass
        return b"", "", url

    async def _send_advanced(self, event: AstrMessageEvent, obmsg: list, fallback_chain: MessageChain, use_forward: bool):
        client = event.bot
        group_id = getattr(event.message_obj, "group_id", None)
        user_id = event.get_sender_id()
        bot_id = str(getattr(client, "self_id", user_id))
        
        # 合并转发模式
        if use_forward:
            obmsg_node = [{
                "type": "node",
                "data": {"name": "虚断", "uin": bot_id, "content": obmsg}
            }]
            try:
                if group_id and hasattr(client, "send_group_forward_msg"):
                    return await client.send_group_forward_msg(group_id=int(group_id), messages=obmsg_node)
                elif hasattr(client, "send_private_forward_msg"):
                    return await client.send_private_forward_msg(user_id=int(user_id), messages=obmsg_node)
            except Exception as e:
                logger.warning(f"[随机图片] 合并转发调用失败，降级常规发送: {e}")

        # 常规发送模式
        try:
            if group_id and hasattr(client, "send_group_msg"):
                return await client.send_group_msg(group_id=int(group_id), message=obmsg)
            elif hasattr(client, "send_private_msg"):
                return await client.send_private_msg(user_id=int(user_id), message=obmsg)
        except Exception: pass
            
        return await event.send(fallback_chain)

    async def _recall_msgs(self, event: AstrMessageEvent, rets: list, delay: int):
        logger.info(f"[随机图片] 撤回倒计时开始: {delay} 秒")
        await asyncio.sleep(delay)
        client = event.bot
        for send_ret in rets:
            if not send_ret: continue
            try:
                msg_id = None
                if isinstance(send_ret, dict): msg_id = send_ret.get("message_id")
                elif hasattr(send_ret, "message_id"): msg_id = getattr(send_ret, "message_id")
                if not msg_id: continue
                if hasattr(client, "delete_msg"): await client.delete_msg(message_id=int(msg_id))
                elif hasattr(client, "api") and hasattr(client.api, "call_action"): await client.api.call_action("delete_msg", message_id=int(msg_id))
                elif hasattr(client, "recall"): await client.recall(msg_id)
            except Exception: pass

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        msg_text = event.message_str.strip()
        sources = self.cfg.get("sources", [])
        matched_source = None
        is_matched = False

        for source in sources:
            if not isinstance(source, dict): continue
            keywords = source.get("keywords", [])
            for kw in keywords:
                kw = str(kw).strip()
                if not kw: continue
                if msg_text == kw or msg_text.startswith(kw + " "):
                    matched_source = source
                    is_matched = True
                    break
            if is_matched: break
        
        if not is_matched or not matched_source: return 

        group_id = getattr(event.message_obj, "group_id", None)
        if group_id:
            group_id_str = str(group_id)
            list_mode = matched_source.get("list_mode", "无限制")
            group_list = [str(x) for x in matched_source.get("group_list", []) if x]
            if list_mode == "白名单" and group_id_str not in group_list: return 
            elif list_mode == "黑名单" and group_id_str in group_list: return 

        event.stop_event()

        user_id = event.get_sender_id()
        remaining = self._check_cooldown(user_id)
        if remaining > 0:
            yield event.plain_result(self._text(f"冲太快了！请休息 {int(remaining)} 秒再试"))
            return
        
        target_apis = matched_source.get("apis", [])
        if not target_apis:
            yield event.plain_result(self._text(f"图源 [{matched_source.get('name')}] 未配置 API 地址"))
            return

        success = await self._process_and_send(event, target_apis, matched_source)
        if success: self.cooldowns[user_id] = time.time()

    async def _process_and_send(self, event: AstrMessageEvent, api_list: List[str], source_cfg: dict) -> bool:
        ssl_context = aiohttp.TCPConnector(verify_ssl=self.cfg.get("verify_ssl", True))
        
        use_forward = source_cfg.get("use_forward", False)
        max_retries = self.cfg.get("send_retries", 3)
        
        async with aiohttp.ClientSession(connector=ssl_context) as session:
            for idx, api_url in enumerate(api_list):
                api_url = str(api_url).strip()
                if not api_url: continue

                temp_file_path = None

                try:
                    body, ctype, final_url = await self._safe_fetch(session, api_url)
                    if not body: continue

                    if "application/json" in ctype:
                        try:
                            data = json.loads(body.decode('utf-8'))
                            real_img_url = self._extract_url_from_json(data)
                            if real_img_url:
                                body, ctype, final_url = await self._safe_fetch(session, real_img_url)
                        except Exception: continue
                            
                    if not body: continue
                    if "text" in ctype and len(body) < 2000 and body.startswith(b"http"):
                        real_url = body.decode('utf-8').strip()
                        body, ctype, final_url = await self._safe_fetch(session, real_url)
                    if not body: continue

                    body = self._compress_image(body)
                    file_ext = "jpg" 
                    if body[0:4] == b'\x89PNG': file_ext = "png"
                    elif body[0:3] == b'GIF': file_ext = "gif"
                    
                    filename = f"{uuid.uuid4()}.{file_ext}"
                    temp_file_path = os.path.join(self.cache_dir, filename)

                    async with aiofiles.open(temp_file_path, "wb") as f:
                        await f.write(body)

                    send_success = False
                    send_ret = None
                    
                    # 构建本地图片对象
                    file_uri = Path(temp_file_path).absolute().as_uri()
                    obmsg_img = [{'type': 'image', 'data': {'file': file_uri}}]
                    fallback_chain_img = MessageChain([Image.fromFileSystem(temp_file_path)])

                    # 尝试发送图片
                    for attempt in range(max_retries + 1):
                        try:
                            send_ret = await self._send_advanced(event, obmsg_img, fallback_chain_img, use_forward)
                            send_success = True
                            break 
                        except Exception:
                            await asyncio.sleep(1)

                    if send_success:
                        recall_delay = int(source_cfg.get("recall_delay", 0)) if source_cfg.get("recall_delay") else 0
                        if recall_delay > 0:
                            rets_to_recall = [send_ret]
                            notice_text = self._text(f"内容将在 {recall_delay} 秒后自动撤回")
                            obmsg_text = [{'type': 'text', 'data': {'text': notice_text}}]
                            notice_chain = MessageChain([Plain(notice_text)])
                            try:
                                notice_ret = await self._send_advanced(event, obmsg_text, notice_chain, use_forward)
                                if notice_ret: rets_to_recall.append(notice_ret)
                            except Exception: pass
                            asyncio.create_task(self._recall_msgs(event, rets_to_recall, recall_delay))
                        return True
                    
                    # 如果发图失败，执行兜底：发送直链（为了防拦截，兜底强制使用合并转发）
                    fallback_msg = self._text(f"图片发送失败，提供原图直链：\n{final_url}")
                    obmsg_text = [{'type': 'text', 'data': {'text': fallback_msg}}]
                    fallback_chain_text = MessageChain([Plain(fallback_msg)])
                    
                    await self._send_advanced(event, obmsg_text, fallback_chain_text, use_forward=True)
                    return False

                except Exception as e:
                    logger.error(f"处理图源时出现异常: {e}")
                    continue
                finally:
                    if temp_file_path and os.path.exists(temp_file_path):
                        async def delayed_delete(path):
                            await asyncio.sleep(15) 
                            try: os.remove(path)
                            except: pass
                        asyncio.create_task(delayed_delete(temp_file_path))
            else:
                await event.send(MessageChain([Plain(self._text("所有图源均无法连接，或已被屏蔽"))]))
                return False
