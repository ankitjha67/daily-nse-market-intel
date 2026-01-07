from __future__ import annotations

import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import List, Sequence, Tuple


@dataclass
class SMTPConfig:
    host: str
    port: int
    from_env: str
    app_password_env: str
    to: List[str]


def send_email(cfg: SMTPConfig, subject: str, body: str, attachments: Sequence[Tuple[str, bytes, str]]) -> None:
    user = os.getenv(cfg.from_env, "")
    pw = os.getenv(cfg.app_password_env, "")
    if not user or not pw:
        raise RuntimeError(f"Missing env secrets: {cfg.from_env} and/or {cfg.app_password_env}")

    msg = EmailMessage()
    msg["From"] = user
    msg["To"] = ", ".join(cfg.to)
    msg["Subject"] = subject
    msg.set_content(body)

    for filename, content, mime in attachments:
        maintype, subtype = mime.split("/", 1)
        msg.add_attachment(content, maintype=maintype, subtype=subtype, filename=filename)

    with smtplib.SMTP(cfg.host, cfg.port) as s:
        s.ehlo()
        s.starttls()
        s.login(user, pw)
        s.send_message(msg)
