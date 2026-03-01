import express from "express";
import identifyRoutes from "./routes/identify.routes.js";

const app = express();

app.use(express.json());

app.use("/identify", identifyRoutes);

export default app;