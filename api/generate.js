import { generateAM } from "../fixbulk.js";

export default async function handler(req, res) {
  if (req.method === "OPTIONS") {
    res.setHeader("Allow", "POST, OPTIONS");
    return res.status(204).end();
  }

  if (req.method !== "POST") {
    res.setHeader("Allow", "POST, OPTIONS");
    return res.status(405).json({
      status: false,
      error: "Method not allowed"
    });
  }

  try {
    const account = await generateAM();

    res.setHeader("Cache-Control", "no-store");
    return res.status(200).json(account);
  } catch (err) {
    return res.status(502).json({
      status: false,
      error: err?.message || "Gagal membuat akun"
    });
  }
}
