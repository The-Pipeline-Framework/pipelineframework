import { AlertCircle, CheckCircle2, Info, TriangleAlert } from "lucide-react";

const icons = {
  success: CheckCircle2,
  error: AlertCircle,
  warning: TriangleAlert,
  info: Info
};

export default function StatusNotice({ tone = "info", title, children }) {
  const Icon = icons[tone] || Info;
  return (
    <section className={`notice ${tone}`}>
      <Icon aria-hidden="true" size={20} />
      <div>
        <h2>{title}</h2>
        <div className="notice-body">{children}</div>
      </div>
    </section>
  );
}
