import os
import ssl

from catcher.steps.external_step import ExternalStep
from catcher.steps.step import Step, update_variables


class Message:
    def __init__(self, message) -> None:
        self.sent_from = message.sent_from  # recipient's email
        self.sent_to = message.sent_to  # receiver's email
        self.subject = message.subject  # email subject
        self.headers = message.headers  # email headers
        self.id = message.message_id  # email id
        self.date = message.date  # date of sending
        self.text = '\n'.join(message.body.get('plain', []))  # plain text body if any
        self.html = '\n'.join(message.body.get('html', []))  # html body if any
        self.attachments = message.attachments  # attachments


class Email(ExternalStep):
    """
    Allows you to send and receive emails via `IMAP <https://en.wikipedia.org/wiki/Internet_Message_Access_Protocol>`_
    protocol.

    :Config:

    - host: mailserver's host
    - port: mailserver's host. *Optional*. Default is 993.
    - user: your username
    - pass: your password
    - ssl: use tls. *Optional* Default is true.
    - starttls: use starttls. *Optional* Default is false.

    :Filter: search filter object. All fields are optional. For more details and filter options please see the readme's
             of https://github.com/martinrusev/imbox library.

    - unread: boolean. If true will get only unread messages. Default is false.
    - sent_from: Get only messages sent from this address.
    - sent_to: Get only messages sent to this address.
    - date__lt: Get messages received before specific date.
    - date__gt: Get messages received after specific date.
    - date__on: Get messages received on a specific date.
    - subject: Get messages whose subjects contain specified string.
    - folder: Get messages from a specific folder.

    :Input:

    :receive: get a list of messages, matching search criteria. From recent to old.

    - config: email's config object.
    - filter: add search filter. *Optional*.
    - ack: mark as read. *Optional* Default is false.
    - limit: limit return result to N messages. *Optional* Default is unlimited.
             Only messages who fit the limit will be marked as read, if ack is true.

    :send: send an email

    - config: email's config object.
    - from: from email
    - to: to email or list of emails
    - cc: list of cc. *Optional*
    - bcc: list of bcc. *Optional*
    - subject: subject. *Optional* Default is empty string.
    - plain: message's text. *Optional*
    - html: message's text in html format. *Optional* Either `plain` or `html` should present.
    - attachments: list with attachment filenames from resources dir. *Optional*

    :message: for fields, available in message please see :class:`.Message`

    :Examples:

    Read all messages, take the last one and check subject
    ::

        variables:
            email_config:
                host: 'imap.google.com'
                user: 'my_user@google.com'
                pass: 'my_pass'
        steps:
            - email:
                receive:
                    conf: '{{ email_config }}'
                register: {last_mail: '{{ OUTPUT[0] }}'}
            - check: {equals: {the: '{{ last_mail.subject }}', is: 'Test Subject'}}

    Read 2 last unread messages and mark them read
    ::

        - email:
              receive:
                  config: '{{ email_conf }}'
                  filter: {unread: true}
                  ack: true
                  limit: 2

    Find unread message containing blog name in subject and mark as read
    ::

        - email:
              receive:
                  config: '{{ email_conf }}'
                  filter: {unread: true, subject: 'justtech.blog'}
                  ack: true
                  limit: 1

    Send message in html format
    ::

        - email:
              send:
                  config: '{{ email_conf }}'
                  to: 'test@test.com'
                  from: 'me@test.com'
                  subject: 'test_subject'
                  html: '
                  <html>
                      <body>
                        <p>Hi,<br>
                           How are you?<br>
                           <a href="http://example.com">Link</a>
                        </p>
                      </body>
                  </html>'

    """

    @update_variables
    def action(self, includes: dict, variables: dict) -> dict or tuple:
        body = self.simple_input(variables)
        method = Step.filter_predefined_keys(body)  # send/receive
        conf = body[method]
        if method == 'send':
            return variables, self.send(conf, variables)
        elif method == 'receive':
            return variables, self.receive(conf)
        else:
            raise AttributeError('unknown method: ' + method)

    def receive(self, body: dict):
        from imbox import Imbox
        conf = body['config']
        with Imbox(conf['host'],
                   port=int(conf.get('port', 993)),
                   username=conf['user'],
                   password=conf['pass'],
                   ssl=conf.get('ssl', True),
                   starttls=conf.get('starttls', False)) as imbox:
            messages = imbox.messages(**body.get('filter', {}))
            # get and reverse
            msg = [(uid, self.msg_to_dict(message)) for uid, message in messages]
            msg.reverse()
            # limit
            if body.get('limit'):
                msg = msg[:body['limit']]
            # mark selected as read
            for uid, message in msg:
                if body.get('ack', False):
                    imbox.mark_seen(uid)
            return list(dict(msg).values())

    @classmethod
    def send(cls, body: dict, variables: dict):
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        conf = body['config']

        message = MIMEMultipart("alternative")
        message["Subject"] = "multipart test"
        message["From"] = body['from']
        message["To"] = body['to']
        message["Cc"] = body.get('cc')
        message["Bcc"] = body.get('bcc')

        if 'plain' in body:
            plain = MIMEText(str(body['plain']), 'plain')
            message.attach(plain)
        else:
            plain = None
        if 'html' in body:
            html = MIMEText(body['html'], 'html')
            message.attach(html)
        else:
            html = None
        if not plain and not html:
            raise ValueError('Either plain or html should exist!')
        cls.add_attachments(body, message, variables)

        with cls.get_smtp_connection(conf) as server:
            server.sendmail(body['from'], body['to'], message.as_string())
        return True

    @classmethod
    def msg_to_dict(cls, message):
        return Message(message)

    @staticmethod
    def get_smtp_connection(conf: dict):
        import smtplib
        context = ssl.create_default_context()
        if conf.get('ssl', True):
            connection = smtplib.SMTP_SSL(conf['host'], int(conf.get('port', 587)), context=context)
        else:
            connection = smtplib.SMTP(conf['host'], int(conf.get('port', 587)))
        if conf.get('starttls', False):
            connection.starttls(context=context)
        connection.login(conf['user'], conf['pass'])
        return connection

    @classmethod
    def add_attachments(cls, body: dict, message, variables):
        resources = variables['RESOURCES_DIR']
        for attachment in body.get('attachments', []):
            part = cls.add_attachment(resources, attachment)
            message.attach(part)

    @staticmethod
    def add_attachment(resources, attachment):
        # TODO templating support?
        from email import encoders
        from email.mime.base import MIMEBase
        with open(os.path.join(resources, attachment), "rb") as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            "attachment; filename= " + attachment,
        )
        return part
