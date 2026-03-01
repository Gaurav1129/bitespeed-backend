import pool from "../config/db.js";
import { normalizeEmail, removeNulls } from "../utils/helpers.js";

export const processIdentify = async ({ email, phoneNumber }) => {
  if (!email && !phoneNumber) {
    throw new Error("Either email or phoneNumber required");
  }

  email = normalizeEmail(email);

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    //  Find matching contacts
    const matchQuery = `
      SELECT * FROM Contact
      WHERE email = $1 OR phoneNumber = $2
    `;
    const { rows: matches } = await client.query(matchQuery, [
      email,
      phoneNumber
    ]);

    //  No matches → create primary
    if (matches.length === 0) {
      const insertQuery = `
        INSERT INTO Contact (email, phoneNumber, linkPrecedence)
        VALUES ($1, $2, 'primary')
        RETURNING *
      `;
      const { rows } = await client.query(insertQuery, [
        email,
        phoneNumber
      ]);

      await client.query("COMMIT");

      return {
        contact: {
          primaryContactId: rows[0].id,
          emails: removeNulls([email]),
          phoneNumbers: removeNulls([phoneNumber]),
          secondaryContactIds: []
        }
      };
    }

    //  Get all primary IDs involved
    const primaryIds = new Set();

    matches.forEach((c) => {
      if (c.linkprecedence === "primary") {
        primaryIds.add(c.id);
      } else {
        primaryIds.add(c.linkedid);
      }
    });

    const primaryIdArray = Array.from(primaryIds);

    const { rows: primaries } = await client.query(
      `SELECT * FROM Contact WHERE id = ANY($1)`,
      [primaryIdArray]
    );

    primaries.sort(
      (a, b) => new Date(a.createdat) - new Date(b.createdat)
    );

    const finalPrimary = primaries[0];

    //  Merge if multiple primaries
    for (let p of primaries.slice(1)) {
      await client.query(
        `UPDATE Contact
         SET linkedId=$1, linkPrecedence='secondary'
         WHERE id=$2`,
        [finalPrimary.id, p.id]
      );

      await client.query(
        `UPDATE Contact
         SET linkedId=$1
         WHERE linkedId=$2`,
        [finalPrimary.id, p.id]
      );
    }

    //  Fetch full cluster
    const { rows: cluster } = await client.query(
      `SELECT * FROM Contact
       WHERE id=$1 OR linkedId=$1`,
      [finalPrimary.id]
    );

    const emails = cluster.map((c) => c.email);
    const phones = cluster.map((c) => c.phonenumber);

    // Create secondary if new info
    if (
      (email && !emails.includes(email)) ||
      (phoneNumber && !phones.includes(phoneNumber))
    ) {
      await client.query(
        `INSERT INTO Contact (email, phoneNumber, linkedId, linkPrecedence)
         VALUES ($1,$2,$3,'secondary')`,
        [email, phoneNumber, finalPrimary.id]
      );
    }

    // Re-fetch cluster after possible insert
    const { rows: finalCluster } = await client.query(
      `SELECT * FROM Contact
       WHERE id=$1 OR linkedId=$1`,
      [finalPrimary.id]
    );

    const finalEmails = removeNulls(
      finalCluster.map((c) => c.email)
    );

    const finalPhones = removeNulls(
      finalCluster.map((c) => c.phonenumber)
    );

    const secondaryIds = finalCluster
      .filter((c) => c.linkprecedence === "secondary")
      .map((c) => c.id);

    await client.query("COMMIT");

    return {
      contact: {
        primaryContactId: finalPrimary.id,
        emails: finalEmails,
        phoneNumbers: finalPhones,
        secondaryContactIds: secondaryIds
      }
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};