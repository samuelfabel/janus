import "./style.css";

const year = new Date().getFullYear();
const footer = document.querySelector(".footer p");
if (footer && !footer.dataset.yearApplied) {
  footer.dataset.yearApplied = "1";
  footer.insertAdjacentText("beforeend", ` · ${year}`);
}
