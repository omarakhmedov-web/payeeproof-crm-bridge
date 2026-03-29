const CFG = {
  SHEET_NAME: 'Leads',
  SHARED_SECRET: 'CHANGE_ME',
  OWNER_EMAIL: 'hello@payeeproof.com',
  SEND_EMAIL_ALERTS: true,
};

function _json(data) {
  return ContentService
    .createTextOutput(JSON.stringify(data))
    .setMimeType(ContentService.MimeType.JSON);
}

function _safe(v) {
  return String(v == null ? '' : v).trim();
}

function _ensureSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(CFG.SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(CFG.SHEET_NAME);
  }

  const headers = [
    'created_at',
    'event',
    'product',
    'lead_id',
    'request_id',
    'name',
    'company',
    'email',
    'volume',
    'notes',
    'origin',
    'source_ip',
    'user_agent',
    'site',
    'api_base',
    'lead_url',
  ];

  if (sheet.getLastRow() === 0) {
    sheet.appendRow(headers);
    sheet.setFrozenRows(1);
  } else {
    const firstRow = sheet.getRange(1, 1, 1, headers.length).getValues()[0];
    const mismatch = headers.some((h, i) => String(firstRow[i] || '') !== h);
    if (mismatch) {
      sheet.insertRows(1, 1);
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      sheet.setFrozenRows(1);
    }
  }

  return sheet;
}

function doPost(e) {
  try {
    const rawBody = (e && e.postData && e.postData.contents) ? e.postData.contents : '';
    if (!rawBody) {
      return _json({ ok: false, status: 'invalid', error: 'EMPTY_BODY' });
    }

    const body = JSON.parse(rawBody);
    if (_safe(body.shared_secret) !== CFG.SHARED_SECRET) {
      return _json({ ok: false, status: 'forbidden', error: 'INVALID_SECRET' });
    }

    const lead = body.lead || {};
    const requestId = _safe(lead.request_id);
    if (!requestId) {
      return _json({ ok: false, status: 'invalid', error: 'REQUEST_ID_REQUIRED' });
    }

    const sheet = _ensureSheet();
    const lastRow = sheet.getLastRow();
    if (lastRow > 1) {
      const existingIds = sheet.getRange(2, 5, lastRow - 1, 1).getValues().flat().map(_safe);
      const existingIndex = existingIds.indexOf(requestId);
      if (existingIndex !== -1) {
        return _json({
          ok: true,
          status: 'duplicate',
          request_id: requestId,
          row_id: 'row_' + String(existingIndex + 2),
        });
      }
    }

    const row = [
      _safe(lead.created_at),
      _safe(body.event || 'pilot_request_created'),
      _safe(body.product || 'payeeproof'),
      _safe(lead.lead_id),
      requestId,
      _safe(lead.name),
      _safe(lead.company),
      _safe(lead.email),
      _safe(lead.volume),
      _safe(lead.notes),
      _safe(lead.origin),
      _safe(lead.source_ip),
      _safe(lead.user_agent),
      _safe(lead.site),
      _safe(lead.api_base),
      _safe(lead.lead_url),
    ];

    sheet.appendRow(row);
    const rowNumber = sheet.getLastRow();

    if (CFG.SEND_EMAIL_ALERTS && CFG.OWNER_EMAIL) {
      MailApp.sendEmail({
        to: CFG.OWNER_EMAIL,
        subject: 'New PayeeProof lead — ' + (_safe(lead.company) || _safe(lead.email) || requestId),
        htmlBody:
          '<p><strong>New PayeeProof lead</strong></p>' +
          '<p><strong>Company:</strong> ' + _safe(lead.company) + '<br>' +
          '<strong>Name:</strong> ' + _safe(lead.name) + '<br>' +
          '<strong>Email:</strong> ' + _safe(lead.email) + '<br>' +
          '<strong>Volume:</strong> ' + _safe(lead.volume) + '<br>' +
          '<strong>Request ID:</strong> ' + requestId + '</p>' +
          '<p><strong>Notes:</strong><br>' + _safe(lead.notes).replace(/\n/g, '<br>') + '</p>',
      });
    }

    return _json({
      ok: true,
      status: 'stored',
      request_id: requestId,
      row_id: 'row_' + String(rowNumber),
    });
  } catch (err) {
    return _json({
      ok: false,
      status: 'failed',
      error: String(err && err.message ? err.message : err),
    });
  }
}
