document.addEventListener('DOMContentLoaded', function () {
    function openModal(target) {
        if (!target) {
            return;
        }
        target.classList.add('show');
        target.setAttribute('aria-hidden', 'false');
        document.body.style.overflow = 'hidden';
    }

    function closeModal(target) {
        if (!target) {
            return;
        }
        target.classList.remove('show');
        target.setAttribute('aria-hidden', 'true');
        document.body.style.overflow = '';
    }

    document.querySelectorAll('[data-bs-toggle="modal"]').forEach(function (trigger) {
        trigger.addEventListener('click', function () {
            const selector = this.getAttribute('data-bs-target');
            openModal(document.querySelector(selector));
        });
    });

    document.querySelectorAll('[data-bs-dismiss="modal"]').forEach(function (trigger) {
        trigger.addEventListener('click', function () {
            closeModal(this.closest('.modal'));
        });
    });

    document.querySelectorAll('.modal').forEach(function (modal) {
        modal.addEventListener('click', function (event) {
            if (event.target === modal) {
                closeModal(modal);
            }
        });
    });

    document.querySelectorAll('[data-bs-dismiss="alert"]').forEach(function (trigger) {
        trigger.addEventListener('click', function () {
            const alert = this.closest('.alert');
            if (alert) {
                alert.remove();
            }
        });
    });

    document.querySelectorAll('[data-bs-toggle="collapse"]').forEach(function (trigger) {
        trigger.addEventListener('click', function () {
            const selector = this.getAttribute('data-bs-target');
            const target = document.querySelector(selector);
            if (target) {
                target.classList.toggle('show');
            }
        });
    });

    document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape') {
            const openModalElement = document.querySelector('.modal.show');
            if (openModalElement) {
                closeModal(openModalElement);
            }
        }
    });
});
