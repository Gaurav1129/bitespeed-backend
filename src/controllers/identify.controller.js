import { processIdentify } from "../services/identify.service.js";

export const identifyContact = async (req, res) => {
  try {
    const result = await processIdentify(req.body);
    return res.status(200).json(result);
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
};